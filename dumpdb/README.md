# dumpdb image
### The database part for [kcdump](https://github.com/mauricioscastro/kcdump)
This is a extended image of the original Postgres with [init scripts](dumpdb-init.sql) that will enable the output of a `kcdump -nologs -dontSplitgv -format json_lines -gzip` to be  loaded into a ephemeral database for SQL manipulation.

All the files contained in `-targetDir` directory of the kcdump command will become each a different schema named after the file name with a table called *'cluster'* and materialized views will be created one for each resource found to have at least one created object in the cluster. Meaning, there will be materialized views called *'pods_v1'*, *'namespaces_v1'* and so on. 

Run with `podman run -it --rm -v ~/.kube/kcdump:/kcdump --name dumpdb quay.io/hcreport/dumpdb postgres` optionally exposing the db port `-p 5432:5432` and remembering the default target directory of the kcdump command is *~/.kube/kcdump*, if you used another you have to mount it in the container.

Once the postgres db is running you can `podman exec dumpdb psql -c "select f.load_cluster_data('/kcdump')"` where, of course, */kcdump* is where the output files are.

Having loaded the cluster data you will be able to issue queries like:

```sql
select name,
       namespace,
       f.jptxt(_, '$.spec.containers[*].resources')      resources,
       f.jptxt(_, '$.spec.containers[*].readinessProbe') readiness,
       f.jptxt(_, '$.spec.containers[*].livenessProbe')  liveness
from 
    api_xxx_brazilsouth_aroapp_io_6443.pods_v1
where
    namespace !~ 'openshift.*' and
    namespace !~ 'open-cluster.*';
```

To build it from the git repo base directory:
```bash
> cd dumpdb
> podman build . -t dumpdb:latest
```

Have fun!