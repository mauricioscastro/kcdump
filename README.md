# kcdump
### A k8s cluster dumper

This comes from [a operator I am building to pull reports from k8s clusters](https://github.com/mauricioscastro/hcreport/tree/dev) and it ended up as a reasonable command line binary executable so I decided to put it apart here. 

Cutting to the chase and leaving explanations for later. It will work like any ordinary kube client and depend on a kubeconfig. Get the command line options with -h.

```bash
> kcdump -h
```

| Option | Description |
| ----------- | ----------- |
| asyncChunkMap | a map of string to int. name.gv -> list chunk size. for the resources acquired in parallel with the desired chunk size. see --defaultChunkSize and --asyncWorkers (default [events.v1=100,events.events.k8s.io/v1=100]) |
| asyncWorkers | number of group version kind to process in parallel (default 10) |
| context | kube config context to use (default ".current-context") |
| defaultChunkSize | number of list items to retrieve until finished for all async workers (default 25) |
| escapeJson | escape Json encoded strings. for some k8s resources , Json encoded content can be found inside values of certain keys and this would break the db bulk load process for a json column. this will render an invalid json document since it's going to have its strings doubly escaped if special chars are found, \t \n ... (default true) |
| format | output format. use one of: 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'. (default "json_lines") |
| getlogs | get pod's logs? (default false) |
| gvk | print (filtered or not) group version kind with format 'gv,k' and exit (default false) |
| gzip | gzip output (default true) |
| kubeconfig | kubeconfig file or read from stdin. (default "USER_HOME/.kube/config") |
| logLevel | use one of: 'info', 'warn', 'error', 'debug', 'panic', 'fatal' (default "error") |
| ns | print (filtered or not) namespaces list and exit (default false) |
| prune | prunes targetDir/cluster_info_port/ after archiving. implies tgz option. if tgz option is not used it does nothing (default false) |
| splitgv | split groupVersion in separate files. when false will force splitns=false. only -format 'yaml' or 'json_lines' accepted. ignores -tgz. a big file is created with everything inside (default false) |
| splitns | split namespaced items into directories with their namespace name (default false) |
| syncChunkMap | a map of string to int. name.gv -> list chunk size. for the resources acquired one by one with the desired chunk size before anything else. see --defaultChunkSize (default [customresourcedefinitions.apiextensions.k8s.io/v1=1,configmaps.v1=1,packagemanifests.packages.operators.coreos.com/v1=1,apirequestcounts.apiserver.openshift.io/v1=1]) |
| targetDir | target directory where the extracted cluster data goes. directory will be recreated from scratch. a sub directory named 'cluster_info_port' is created inside the targetDir. (default "/home/macastro/.kube/kcdump") |
| tgz | a gzipped tar file is created at targetDir level with its contents. will turn off gzip option (default false) |
| xgvk | regex to match and exclude unwanted groupVersion and kind. format is 'gv:k' where gv is regex to capture gv and k is regex to capture kind. ex: -xgvk "metrics.*:Pod.*". can be used multiple times and/or many items separated by comma -xgvk "metrics.*:Pod.*,.*:Event.*" |
| xns | regex to match and exclude unwanted namespaces. can be used multiple times and/or many items separated by comma -xns "open-.*,kube.*" |
### How I use it in the operator
In the operator it is lauched as a Job with the default options for which the command line counterpart would be:
```bash
> kcdump --splitgv=false --format=json_lines --getlogs=false --gzip=true --escapeJson=true
```
Those are the default options, similar to just calling `> kcdump` . With this, a big gziped json file is created. This big json is later loaded by the [dumpdb](./dumpdb/) container for manipulation with Postgres SQL queries.

### Run the container version
```bash
> podman run --rm --name kcdump quay.io/hcreport/kcdump -h
```
or build it locally:
```bash
> make container-build IMG=kcdump
```
