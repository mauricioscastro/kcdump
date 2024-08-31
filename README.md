# kcdump
### A k8s cluster dumper

This comes from [a operator I am building to pull reports from k8s clusters](https://github.com/mauricioscastro/hcreport/tree/dev) and it ended up as a reasonable command line binary executable so I decided to put it apart here. 

Cutting to the chase and leaving explanations for later. It will work like any ordinary kube client and depend on a kubeconfig. Get the command line options with -h.

```bash
> kcdump -h
```

### Options
`--async-chunk-map` a map of string to int. name.gv -> list chunk size. for the resources acquired in parallel with the desired chunk size. see --default-chunk-size and --async-workers (default [events.v1=100,events.events.k8s.io/v1=100])

`--async-workers` number of group version kind to process in parallel (default 8)

`-f,--config` kcdump config file. command line options have precedence (default "USER_HOME/.kube/kcdump/kcdump.yaml")

`--context` kube config context to use (default ".current-context")

`--copy-to-pod` if the result of the dump is a file. a gziped json lines or a tar gziped group of directories, copy this result into the given container described as 'namespace/pod/container:/absolute_path_to_destination_file'. pod can be a substring of the target pod for which the first replica found will be used and container can be omitted for which the first container found in the pod manifest will be used. if file path ends with a '/' it will be considered a directory and source file will be copied into it. if file path is omitted all together the file will copied to '/tmp'

`--default-chunk-size` number of list items to retrieve until finished for all async workers (default 25)

`--escapejson` escape Json encoded strings. for some k8s resources , Json encoded content can be found inside values of certain keys and this would break the db bulk load process for a json column. this will render an invalid json document since it's going to have its strings doubly escaped if special chars are found, \t \n ... (default true)

`--filename-prefix` if the result of the dump is a file. a gziped json lines or a tar gziped group of directories, add this prefix to the file name. which will result in prefix'cluster_info_port'[.gz or .tgz]

`--format` output format. use one of: 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'. (default "json_lines")

`--gvk` print (filtered or not) name, group version kind with format 'name,gv,k' and exit (default false)

`--gzip` gzip output (default true)

`--kubeconfig` kubeconfig file or read from stdin. (default "USER_HOME/.kube/config")

`--loglevel` use one of: 'info', 'warn', 'error', 'debug', 'panic', 'fatal' (default "error")

`--ns` print (filtered or not) namespaces list and exit (default false)

`--prune` prunes targetDir/cluster_info_port/ after archiving. implies tgz option. if tgz option is not used it does nothing (default false)

`--splitgv` split groupVersion in separate files. when false: will force splitns=false, will only accepts --format 'yaml' or 'json_lines', ignores -tgz and a big file is created with everything inside (default false)

`--splitns` split namespaced items into directories with their namespace name (default false)

`--sync-chunk-map` a map of string to int. name.gv -> list chunk size. for the resources acquired one by one with the desired chunk size before anything else. see --default-chunk-size (default [configmaps.v1=1,packagemanifests.packages.operators.coreos.com/v1=1,apirequestcounts.apiserver.openshift.io/v1=1,customresourcedefinitions.apiextensions.k8s.io/v1=1])

`--tail-log-lines` number of lines to tail the pod's logs. if -1 infinite. 0 = do not get logs (default 0)

`--targetdir` target directory where the extracted cluster data goes. directory will be recreated from scratch. a sub directory named 'cluster_info_port' is created inside the targetDir. (default "USER_HOME/.kube/kcdump")

`--tgz` a gziped tar file is created at targetDir level with its contents. will turn off gzip option (default false)

`--xgvk` regex to match and exclude unwanted groupVersion and kind. format is 'gv:k' where gv is regex to capture gv and k is regex to capture kind. ex: -xgvk "metrics.\*:Pod.\*". can be used multiple times and/or many items separated by comma -xgvk "metrics.\*:Pod.\*,.\*:Event.\*"

`--xns` regex to match and exclude unwanted namespaces. can be used multiple times and/or many items separated by comma -xns "open-.\*,kube.\*"
### How I use it in the operator
In the operator it is lauched as a Job with the default options for which the command line counterpart would be:
```bash
> kcdump --splitgv=false --format=json_lines --tail-lines=0 --gzip=true --escapejson=true
```
Those are the default options, similar to just calling `> kcdump` . With this, a big gziped json file is created. This big json is later loaded by the [dumpdb](./dumpdb/) container for manipulation with Postgres SQL queries.

### Run the container version
```bash
> podman run --rm --name kcdump quay.io/hcreport/kcdump -h
> podman run --rm --name kcdump -v /tmp:/tmp/kcdump -i \
  quay.io/hcreport/kcdump \
  --loglevel=info --targetdir=/tmp/kcdump < ~/.kube/config
```
or build it locally:
```bash
> make container-build IMG=kcdump
```
