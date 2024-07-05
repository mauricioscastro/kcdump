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

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/mauricioscastro/kcdump/pkg/kc"
	kcli "github.com/mauricioscastro/kcdump/pkg/kc"
	"github.com/mauricioscastro/kcdump/pkg/util/log"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
)

type nsExcludeList []string
type gvkExcludeList []string

var (
	logger = log.Logger().Named("hcr")

	// cli dump options
	home        string
	gzip        bool
	tgz         bool
	prune       bool
	nologs      bool
	ns          bool
	dontsplitns bool
	dontsplitgv bool
	gvk         bool
	xns         nsExcludeList
	xgvk        gvkExcludeList
	targetDir   string
	format      string
	kubeconfig  string
	context     string
	logLevel    string
)

func init() {
	yjq.SilenceYqLogs()
	var err error
	home, err = os.UserHomeDir()
	if err != nil {
		logger.Error("reading home info", zap.Error(err))
		os.Exit(-1)
	}
	if home == "/" {
		home = ""
	}
}

func main() {
	flag.BoolVar(&nologs, "nologs", false, "do not output pod's logs")
	flag.BoolVar(&gzip, "gzip", false, "gzip output")
	flag.BoolVar(&tgz, "tgz", false, "a gzipped tar file is created at targetDir level with its contents. will turn off gzip option.")
	flag.BoolVar(&prune, "prune", false, "prunes targetDir/cluster_info_port_data/ after archiving. implies tgz option. if tgz option is not used it does nothing.")
	flag.BoolVar(&ns, "ns", false, "print (filtered or not) namespaces list and exit")
	flag.BoolVar(&gvk, "gvk", false, "print (filtered or not) group version kind with format 'gv,k' and exit")
	flag.BoolVar(&dontsplitns, "dontSplitns", false, "do not split namespaced items into directories with their namespace name")
	flag.BoolVar(&dontsplitgv, "dontSplitgv", false, "do not split groupVersion in separate files. implies -dontSplitns and -format 'yaml' or 'json_lines'. ignores -tgz. a big file is created with everything inside.")
	flag.Var(&xns, "xns", "regex to match and exclude unwanted namespaces. can be used multiple times.")
	flag.Var(&xgvk, "xgvk", "regex to match and exclude unwanted groupVersion and kind. format is 'gv,k' where gv is regex to capture gv and k is regex to capture kind. ex: -xgvk metrics.*,Pod.*. can be used multiple times.")
	flag.StringVar(&targetDir, "targetDir", filepath.FromSlash(home+"/.kube/kcdump"), "target directory where the extracted cluster data goes. directory will be recreated from scratch. a sub directory named 'cluster_info_port' is created inside the targetDir.")
	flag.StringVar(&format, "format", "yaml", "output format. use one of: 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'.")
	flag.StringVar(&kubeconfig, "kubeconfig", filepath.FromSlash(home+"/.kube/config"), "kubeconfig file or read from stdin.")
	flag.StringVar(&context, "context", kc.CurrentContext, "kube config context to use")
	flag.StringVar(&logLevel, "logLevel", "fatal", "use one of: 'info', 'warn', 'error', 'debug', 'panic', 'fatal'")

	flag.Parse()

	os.Exit(dump())
}

func dump() int {
	log.SetLoggerLevel(logLevel)
	kc := kcli.NewKcWithConfigContext(kubeconfig, context)
	// fmt.Println(kc.Cluster())
	if kc == nil {
		fmt.Fprintf(os.Stderr, "unable to start k8s client from config file '%s' and context '%s'\n", kubeconfig, context)
		os.Exit(-1)
	}
	if ns {
		n, e := kc.Ns()
		if e != nil {
			return 1
		}
		n, e = kcli.FilterNS(n, xns, true)
		if e != nil {
			return 2
		}
		n, e = yjq.YqEval("[.items[].metadata.name] | sort | .[]", n)
		if e != nil {
			return 3
		}
		// fmt.Println("namespace")
		fmt.Println(n)
	}
	if gvk {
		g, e := kc.ApiResources()
		if e != nil {
			return 4
		}
		g, e = kcli.FilterApiResources(g, xgvk)
		if e != nil {
			return 5
		}
		g, e = yjq.YqEval(`with(.items[]; .verbs = (.verbs | to_entries)) | .items[] | select(.available and .verbs[].value == "get") | [.groupVersion + "," + .kind] | sort | .[]`, g)
		if e != nil {
			return 6
		}
		if ns {
			fmt.Println()
		}
		// fmt.Println("groupVersion,kind")
		fmt.Println(g)
	}
	if !ns && !gvk {
		outputfmt, e := kcli.FormatCodeFromString(format)
		if e != nil {
			fmt.Fprintf(os.Stderr, "%s\n", e.Error())
			return 7
		}
		if e = kc.Dump(targetDir, xns, xgvk, nologs, gzip, tgz, prune, !dontsplitns, !dontsplitgv, outputfmt, 0, nil); e != nil {
			fmt.Fprintf(os.Stderr, "%s\n", e.Error())
			return 9
		}
	}
	return 0
}

func (i *nsExcludeList) String() string {
	return fmt.Sprint(*i)
}

func (i *nsExcludeList) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func (i *gvkExcludeList) String() string {
	return fmt.Sprint(*i)
}

func (i *gvkExcludeList) Set(value string) error {
	*i = append(*i, value)
	return nil
}
