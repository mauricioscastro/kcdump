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
	"strings"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/coreybutler/go-fsutil"
	Kc "github.com/mauricioscastro/kcdump/pkg/kc"
	"github.com/mauricioscastro/kcdump/pkg/util/log"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	logger     = log.Logger().Named("kcdump.main")
	totalGvk   = 0
	gvkCounter atomic.Int32

	// cli dump options
	home             string
	gzip             bool
	tgz              bool
	prune            bool
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
	config           string
	copyToPod        string
	filenamePrefix   string
	tailLines        int
	showProgress     bool
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

// readme.md: go run cmd/main.go -h 2>&1 | grep -v -e Usage -e help -e  "exit status" | sed -e 's/^  *//g' -e 's/, -/,-/g' | cut -d ' ' -f 1,3- | sed -e 's/  */ /g' | sed -E 's/^(-[^ ]+) (.*)$/`\1` \2\n/g' | sed -E 's,/home/.*/.kube/(.*),USER_HOME/.kube/\1,g' | sed -e 's/\*/\\*/g'

func main() {
	pflag.BoolVar(&gzip, "gzip", true, "gzip output")
	pflag.BoolVar(&tgz, "tgz", false, "a gziped tar file is created at targetDir level with its contents. will turn off gzip option (default false)")
	pflag.BoolVar(&prune, "prune", false, "prunes targetDir/cluster_info_port/ after archiving. implies tgz option. if tgz option is not used it does nothing (default false)")
	pflag.BoolVar(&ns, "ns", false, "print (filtered or not) namespaces list and exit (default false)")
	pflag.BoolVar(&gvk, "gvk", false, "print (filtered or not) name, group version kind with format 'name,gv,k' and exit (default false)")
	pflag.BoolVar(&splitns, "splitns", false, "split namespaced items into directories with their namespace name (default false)")
	pflag.BoolVar(&splitgv, "splitgv", false, "split groupVersion in separate files. when false: will force splitns=false, will only accepts --format 'yaml' or 'json_lines', ignores -tgz and a big file is created with everything inside (default false)")
	pflag.StringSliceVar(&xns, "xns", []string{}, `regex to match and exclude unwanted namespaces. can be used multiple times and/or many items separated by comma -xns "open-.*,kube.*"`)
	pflag.StringSliceVar(&xgvk, "xgvk", []string{}, `regex to match and exclude unwanted groupVersion and kind. format is 'gv:k' where gv is regex to capture gv and k is regex to capture kind. ex: -xgvk "metrics.*:Pod.*". can be used multiple times and/or many items separated by comma -xgvk "metrics.*:Pod.*,.*:Event.*"`)
	pflag.StringVar(&targetDir, "targetdir", filepath.FromSlash(home+"/.kube/kcdump"), "target directory where the extracted cluster data goes. directory will be recreated from scratch. a sub directory named 'cluster_info_port' is created inside the targetDir.")
	pflag.StringVar(&format, "format", "json_lines", "output format. use one of: 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'.")
	pflag.BoolVar(&escapeJson, "escapejson", true, "escape Json encoded strings. for some k8s resources , Json encoded content can be found inside values of certain keys and this would break the db bulk load process for a json column. this will render an invalid json document since it's going to have its strings doubly escaped if special chars are found, \\t \\n ...")
	pflag.StringVar(&kubeconfig, "kubeconfig", filepath.FromSlash(home+"/.kube/config"), "kubeconfig file or read from stdin.")
	pflag.StringVar(&context, "context", Kc.CurrentContext, "kube config context to use")
	pflag.StringVar(&logLevel, "loglevel", "error", "goes to stderr. use one of: 'info', 'warn', 'error', 'debug', 'panic', 'fatal'")
	pflag.IntVar(&asyncWorkers, "async-workers", 8, "number of group version kind to process in parallel")
	pflag.IntVar(&defaultChunkSize, "default-chunk-size", 25, "number of list items to retrieve until finished for all async workers")
	pflag.StringToIntVar(&syncChunkMap, "sync-chunk-map", syncChunkMap, "a map of string to int. name.gv -> list chunk size. for the resources acquired one by one with the desired chunk size before anything else. see --default-chunk-size")
	pflag.StringToIntVar(&asyncChunkMap, "async-chunk-map", asyncChunkMap, "a map of string to int. name.gv -> list chunk size. for the resources acquired in parallel with the desired chunk size. see --default-chunk-size and --async-workers")
	pflag.StringVarP(&config, "config", "f", filepath.FromSlash(home+"/.kube/kcdump/kcdump.yaml"), "kcdump config file. command line options have precedence")
	pflag.StringVar(&copyToPod, "copy-to-pod", "", "if the result of the dump is a file. a gziped json lines or a tar gziped group of directories, copy this result into the given container described as 'namespace/pod/container:/absolute_path_to_destination_file'. pod can be a substring of the target pod for which the first replica found will be used and container can be omitted for which the first container found in the pod manifest will be used. if file path ends with a '/' it will be considered a directory and source file will be copied into it. if file path is omitted all together the file will copied to '/tmp'")
	pflag.StringVar(&filenamePrefix, "filename-prefix", "", "if the result of the dump is a file. a gziped json lines or a tar gziped group of directories, add this prefix to the file name. which will result in prefix'cluster_info_port'[.gz or .tgz]")
	pflag.IntVar(&tailLines, "tail-log-lines", 0, "number of lines to tail the pod's logs. if -1 infinite. 0 = do not get logs (default 0)")
	pflag.BoolVar(&showProgress, "show-progress", false, "show percentage completed in stdout")
	pflag.Parse()

	log.SetLoggerLevel(logLevel)

	viper.BindPFlags(pflag.CommandLine)
	viper.SetConfigFile(config)

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			logger.Info("no configuration file found")
		} else {
			logger.Info("configuration file " + config + " not loaded. using default parameters from command line")
		}
	}
	if err := optionsFromViper(); err != nil {
		logger.Error("reading options", zap.Error(err))
		os.Exit(11)
	}

	os.Exit(dump())
}

func dump() int {
	log.SetLoggerLevel(logLevel)
	ll := os.Getenv("LOGGER_LEVEL")
	if ll != "" {
		log.SetLoggerLevel(ll)
	}
	var kc Kc.Kc = nil
	if fsutil.Exists(Kc.TokenPath) &&
		os.Getenv("KUBERNETES_SERVICE_HOST") != "" &&
		os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS") != "" { // running in cluster
		kc = Kc.NewKc()
	} else { // running solo
		kc = Kc.NewKcWithConfigContext(kubeconfig, context)
	}
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
	gvkCounter.Store(0)
	progress := func() {
		p := (gvkCounter.Add(1) * 100) / int32(totalGvk)
		logger.Sugar().Info("PROGRESS ", p, "%")
		if showProgress {
			fmt.Printf("PROGRESS %d%%\r", p)
		}
	}
	if showProgress {
		fmt.Print("PROGRESS 0%\r")
	}
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
	if gvk {
		fmt.Println(g)
		return 0
	}
	totalGvk = len(strings.Split(g, "\n"))
	if e := kc.Dump(
		targetDir,
		xns,
		xgvk,
		syncChunkMap,
		asyncChunkMap,
		gzip,
		tgz,
		prune,
		splitns,
		splitgv,
		outputfmt,
		asyncWorkers,
		defaultChunkSize,
		escapeJson,
		copyToPod,
		filenamePrefix,
		tailLines,
		progress); e != nil {
		fmt.Fprintf(os.Stderr, "%s\n", e.Error())
		return 9
	}
	return 0
}

func optionsFromViper() error {
	var err error = nil
	gzip = viper.GetBool("gzip")
	tgz = viper.GetBool("tgz")
	prune = viper.GetBool("prune")
	ns = viper.GetBool("ns")
	splitns = viper.GetBool("splitns")
	splitgv = viper.GetBool("splitgv")
	gvk = viper.GetBool("gvk")
	xns = viper.GetStringSlice("xns")
	xgvk = viper.GetStringSlice("xgvk")
	targetDir = viper.GetString("targetdir")
	format = viper.GetString("format")
	escapeJson = viper.GetBool("escapejson")
	kubeconfig = viper.GetString("kubeconfig")
	context = viper.GetString("context")
	logLevel = viper.GetString("loglevel")
	copyToPod = viper.GetString("copy-to-pod")
	filenamePrefix = viper.GetString("filename-prefix")
	asyncWorkers = viper.GetInt("async-workers")
	defaultChunkSize = viper.GetInt("default-chunk-size")
	tailLines = viper.GetInt("tail-log-lines")
	showProgress = viper.GetBool("show-progress")
	if syncChunkMap, err = getStringMapInt(viper.Get("sync-chunk-map")); err != nil {
		return err
	}
	if asyncChunkMap, err = getStringMapInt(viper.Get("async-chunk-map")); err != nil {
		return err
	}
	logger.Sugar().Debug("targetDir=", targetDir, " copyToPod=", copyToPod, " filenamePrefix="+filenamePrefix)
	return err
}

func getStringMapInt(m any) (map[string]int, error) {
	stringMapAny, ok := m.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("value %s is not map[string]any", m)
	}
	result := make(map[string]int)
	for k, v := range stringMapAny {
		if intValue, ok := v.(int); ok {
			result[k] = intValue
		} else {
			return nil, fmt.Errorf("value %s is not int", k)
		}
	}
	return result, nil
}
