# kcdump
### A k8s cluster dumper

This comes from [a operator I am building to pull reports from k8s clusters](https://github.com/mauricioscastro/hcreport/tree/dev) and it ended up as a reasonable command line binary executable so I decided to put it apart here. 

Cutting to the chase and leaving explanations for later. It will work like any ordinary kube client and depend on a kubeconfig. Get the command line options with -h.

```bash
> kcdump -h
```

| Option | Description |
| ----------- | ----------- |
| context  | kube config context to use (default ".current-context") |
| dontSplitgv |	do not split groupVersion in separate files. implies dontSplitns and -format 'yaml' or 'json_lines'. ignores tgz. a big file is created with everything inside. |
| dontSplitns | do not split namespaced items into directories with their namespace name |
| format | output format. use one of: 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'. (default "yaml") |
| gvk | print (filtered or not) group version kind with format 'gv,k' and exit |
| gzip | gzip output |
| kubeconfig | kubeconfig file or read from stdin. (default "/home/user/.kube/config") |
| logLevel | use one of: 'info', 'warn', 'error', 'debug', 'panic', 'fatal' (default "fatal") |
| nologs | do not output pod's logs |
| ns |	print (filtered or not) namespaces list and exit |
| prune | prunes targetDir/cluster_info_port/ after archiving. implies tgz option. if tgz option is not used it does nothing |
| targetDir | target directory where the extracted cluster data goes. directory will be recreated from scratch. a sub directory named 'cluster_info_port' is created inside the targetDir. (default "/home/user/.kube/kcdump") |
| tgz | a gzipped tar file is created at targetDir level with its contents. will turn off gzip option |
| xgvk | regex to match and exclude unwanted groupVersion and kind. format is 'gv,k' where gv is regex to capture gv and k is regex to capture kind. ex: -xgvk metrics.\*,Pod.\*. can be used multiple times |
| xns | regex to match and exclude unwanted namespaces. can be used multiple times |

### How I use it in the operator
In the operator it is used as a library, but the command line counterpart would be:
```bash
> kcdump -nologs -dontSplitgv -format json_lines -gzip
```
With this, a big gziped json file is created. This big json is later loaded by the [dumpdb](./dumpdb/) container for manipulation with Postgres SQL queries.

### Run the container version
```bash
> podman run --rm --name kcdump quay.io/hcreport/kcdump -h
```
or build it locally:
```bash
> make container-build IMG=kcdump
```
