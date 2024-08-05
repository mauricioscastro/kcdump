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
	// "flag"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/mauricioscastro/kcdump/pkg/kc"
	Kc "github.com/mauricioscastro/kcdump/pkg/kc"
	"github.com/mauricioscastro/kcdump/pkg/util/log"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
	flag "github.com/spf13/pflag"
)

var (
	logger = log.Logger().Named("kcdump.main")

	// cli dump options
	home             string
	gzip             bool
	tgz              bool
	prune            bool
	getlogs          bool
	ns               bool
	splitns          bool
	splitgv          bool
	gvk              bool
	xns              []string
	xgvk             []string
	syncChunkMap     map[string]int
	asyncChunkMap    map[string]int
	targetDir        string
	format           string
	escapeJson       bool
	kubeconfig       string
	context          string
	logLevel         string
	asyncWorkers     int
	defaultChunkSize int
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
	syncChunkMap = map[string]int{
		"configmaps.v1": 1,
		"packagemanifests.packages.operators.coreos.com/v1": 1,
		"apirequestcounts.apiserver.openshift.io/v1":        1,
		"customresourcedefinitions.apiextensions.k8s.io/v1": 1,
	}
	asyncChunkMap = map[string]int{
		"events.v1":               100,
		"events.events.k8s.io/v1": 100,
	}
}

func main() {
	flag.BoolVar(&getlogs, "getlogs", false, "get pod's logs? (default false)")
	flag.BoolVar(&gzip, "gzip", true, "gzip output")
	flag.BoolVar(&tgz, "tgz", false, "a gzipped tar file is created at targetDir level with its contents. will turn off gzip option (default false)")
	flag.BoolVar(&prune, "prune", false, "prunes targetDir/cluster_info_port/ after archiving. implies tgz option. if tgz option is not used it does nothing (default false)")
	flag.BoolVar(&ns, "ns", false, "print (filtered or not) namespaces list and exit (default false)")
	flag.BoolVar(&gvk, "gvk", false, "print (filtered or not) group version kind with format 'gv,k' and exit (default false)")
	flag.BoolVar(&splitns, "splitns", false, "split namespaced items into directories with their namespace name (default false)")
	flag.BoolVar(&splitgv, "splitgv", false, "split groupVersion in separate files. when false will force splitns=false. only -format 'yaml' or 'json_lines' accepted. ignores -tgz. a big file is created with everything inside (default false)")
	flag.StringSliceVar(&xns, "xns", []string{}, `regex to match and exclude unwanted namespaces. can be used multiple times and/or many items separated by comma -xns "open-.*,kube.*"`)
	flag.StringSliceVar(&xgvk, "xgvk", []string{}, `regex to match and exclude unwanted groupVersion and kind. format is 'gv:k' where gv is regex to capture gv and k is regex to capture kind. ex: -xgvk "metrics.*:Pod.*". can be used multiple times and/or many items separated by comma -xgvk "metrics.*:Pod.*,.*:Event.*"`)
	flag.StringVar(&targetDir, "targetDir", filepath.FromSlash(home+"/.kube/kcdump"), "target directory where the extracted cluster data goes. directory will be recreated from scratch. a sub directory named 'cluster_info_port' is created inside the targetDir.")
	flag.StringVar(&format, "format", "json_lines", "output format. use one of: 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'.")
	flag.BoolVar(&escapeJson, "escapeJson", true, "escape Json encoded strings. for some k8s resources , Json encoded content can be found inside values of certain keys and this would break the db bulk load process for a json column.")
	flag.StringVar(&kubeconfig, "kubeconfig", filepath.FromSlash(home+"/.kube/config"), "kubeconfig file or read from stdin.")
	flag.StringVar(&context, "context", kc.CurrentContext, "kube config context to use")
	flag.StringVar(&logLevel, "logLevel", "error", "use one of: 'info', 'warn', 'error', 'debug', 'panic', 'fatal'")
	flag.IntVar(&asyncWorkers, "asyncWorkers", 10, "number of group version kind to process in parallel")
	flag.IntVar(&defaultChunkSize, "defaultChunkSize", 25, "number of list items to retrieve until finished for all async workers")
	flag.StringToIntVar(&syncChunkMap, "syncChunkMap", syncChunkMap, "")
	flag.StringToIntVar(&asyncChunkMap, "asyncChunkMap", asyncChunkMap, "")
	flag.Parse()

	os.Exit(dump())
}

func dump() int {
	log.SetLoggerLevel(logLevel)
	kc := Kc.NewKcWithConfigContext(kubeconfig, context)
	if kc == nil {
		fmt.Fprintf(os.Stderr, "unable to start k8s client from config file '%s' and context '%s'\n", kubeconfig, context)
		os.Exit(-1)
	}
	outputfmt, e := Kc.FormatCodeFromString(format)
	if e != nil {
		fmt.Fprintf(os.Stderr, "%s\n", e.Error())
		return 7
	}
	if ns {
		n, e := kc.Ns()
		if e != nil {
			return 1
		}
		n, e = Kc.FilterNS(n, xns, outputfmt, true)
		if e != nil {
			return 2
		}
		n, e = yjq.YqEval("[.items[].metadata.name] | sort | .[]", n)
		if e != nil {
			return 3
		}
		// fmt.Println("namespace")
		fmt.Println(n)
		return 0
	}
	if gvk {
		g, e := kc.ApiResources()
		if e != nil {
			return 4
		}
		g, e = Kc.FilterApiResources(g, xgvk, outputfmt)
		if e != nil {
			return 5
		}
		g, e = yjq.YqEval(`with(.items[]; .verbs = (.verbs | to_entries)) | .items[] | select(.available and .verbs[].value == "get") | [.name + "," + .groupVersion + "," + .kind] | .[]`, g)
		if e != nil {
			return 6
		}
		if ns {
			fmt.Println()
		}
		// fmt.Println("groupVersion,kind")
		fmt.Println(g)
		return 0
	}
	if e := kc.Dump(targetDir, xns, xgvk, syncChunkMap, asyncChunkMap, !getlogs, gzip, tgz, prune, splitns, splitgv, outputfmt, asyncWorkers, defaultChunkSize, escapeJson, nil); e != nil {
		fmt.Fprintf(os.Stderr, "%s\n", e.Error())
		return 9
	}
	return 0
}

// func (i *nsExcludeList) String() string {
// 	return fmt.Sprint(*i)
// }

// func (i *nsExcludeList) Set(value string) error {
// 	*i = append(*i, value)
// 	return nil
// }

// func (i *gvkExcludeList) String() string {
// 	return fmt.Sprint(*i)
// }

// func (i *gvkExcludeList) Set(value string) error {
// 	*i = append(*i, value)
// 	return nil
// }
