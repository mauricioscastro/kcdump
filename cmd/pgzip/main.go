/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// gzip compatible command line wrapper around github.com/klauspost/pgzip
// so (de)compression can be parallelized from outside go code. i.e. from
// postgres 'copy ... from program ”pgzip -dc file.gz”'
package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/klauspost/pgzip"
)

const usage = `usage: pgzip [-cdfkqv] [-1..-9] [file ...]

  -c  write to stdout, keep original files
  -d  decompress
  -f  force overwrite of output file
  -k  keep original files
  -q  quiet
  -v  print version
  -1 .. -9  compression level (default -6)

with no file, or when file is -, read stdin and write stdout.
`

var (
	decompress bool
	toStdout   bool
	force      bool
	keep       bool
	quiet      bool
	level      = pgzip.DefaultCompression
)

func main() {
	files, err := parseArgs(os.Args[1:])
	if err != nil {
		die(err)
	}
	if len(files) == 0 {
		files = []string{"-"}
	}
	for _, f := range files {
		if err = run(f); err != nil {
			die(err)
		}
	}
}

func parseArgs(args []string) ([]string, error) {
	files := []string{}
	for _, a := range args {
		if a == "-" || !strings.HasPrefix(a, "-") {
			files = append(files, a)
			continue
		}
		switch a {
		case "-h", "--help":
			fmt.Print(usage)
			os.Exit(0)
		case "-v", "--version":
			fmt.Println("pgzip (klauspost/pgzip) " + runtime.Version())
			os.Exit(0)
		case "--stdout":
			toStdout = true
		case "--decompress", "--uncompress":
			decompress = true
		case "--force":
			force = true
		case "--keep":
			keep = true
		case "--quiet":
			quiet = true
		default:
			if len(a) == 2 && a[1] >= '1' && a[1] <= '9' {
				level = int(a[1] - '0')
				continue
			}
			// short flags can be bundled. i.e. -dc
			for _, o := range a[1:] {
				switch o {
				case 'c':
					toStdout = true
				case 'd':
					decompress = true
				case 'f':
					force = true
				case 'k':
					keep = true
				case 'q':
					quiet = true
				case 'n', 'N':
					// name handling flags accepted and ignored for gzip compatibility
				default:
					if o >= '1' && o <= '9' {
						level = int(o - '0')
						continue
					}
					return nil, fmt.Errorf("invalid option -- '%c'", o)
				}
			}
		}
	}
	return files, nil
}

func run(file string) error {
	if file == "-" {
		return filter(os.Stdin, os.Stdout)
	}
	in, err := os.Open(file)
	if err != nil {
		return err
	}
	defer in.Close()

	if toStdout {
		return filter(in, os.Stdout)
	}

	target, err := targetName(file)
	if err != nil {
		return err
	}
	if _, err = os.Stat(target); err == nil && !force {
		return fmt.Errorf("%s already exists", target)
	}
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	defer out.Close()

	if err = filter(in, out); err != nil {
		os.Remove(target)
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	if !keep {
		in.Close()
		if err = os.Remove(file); err != nil {
			return err
		}
	}
	if !quiet {
		fmt.Fprintf(os.Stderr, "%s -> %s\n", file, target)
	}
	return nil
}

func targetName(file string) (string, error) {
	if !decompress {
		return file + ".gz", nil
	}
	for _, s := range []string{".gz", ".tgz", "-gz", ".z", ".Z"} {
		if strings.HasSuffix(file, s) {
			if s == ".tgz" {
				return strings.TrimSuffix(file, s) + ".tar", nil
			}
			return strings.TrimSuffix(file, s), nil
		}
	}
	return "", fmt.Errorf("%s: unknown suffix -- ignored", file)
}

func filter(in io.Reader, out io.Writer) error {
	if decompress {
		r, err := pgzip.NewReaderN(in, 1<<20, runtime.NumCPU())
		if err != nil {
			return err
		}
		defer r.Close()
		if _, err = io.Copy(out, r); err != nil {
			return err
		}
		return r.Close()
	}
	w, err := pgzip.NewWriterLevel(out, level)
	if err != nil {
		return err
	}
	defer w.Close()
	if err = w.SetConcurrency(1<<20, runtime.NumCPU()); err != nil {
		return err
	}
	if _, err = io.Copy(w, in); err != nil {
		return err
	}
	return w.Close()
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "pgzip: "+err.Error())
	os.Exit(1)
}
