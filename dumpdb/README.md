# dumpdb image
### The database for [kcdump](https://github.com/mauricioscastro/kcdump)
This is a extended image of the original Postgres with [init scripts](dumpdb-init.sql) that will enable the output of a `> kcdump` to be  loaded into a ephemeral database for SQL manipulation.

All the lines from all the generated files in `--targetDir` directory of the kcdump command will become each entries in a table called *'cluster'*.
A table named *'apiresources'* will be created (based on cluster) with all detected api resources in the cluster (the ones not filtered by kcdump config).
A materialized view with the name *'resources'* will be created (based on cluster) with the resources found in the cluster.

Once the postgres db is running you can `podman exec dumpdb psql -c "select load_cluster_data('/kcdump')"` where, of course, */kcdump* is where the output files are.

Having loaded the cluster data you will be able to issue queries like:

```sql
select 
       cluster_id,
       name,
       namespace,
       jp(_, '$.spec.containers[*].resources')      resources,
       jp(_, '$.spec.containers[*].readinessProbe') readiness,
       jp(_, '$.spec.containers[*].livenessProbe')  liveness
from
    resources
where
    api_id = 'pods' and
    namespace !~ 'openshift.*' and
    namespace !~ 'open-cluster.*'
order by
    namespace, name;
```
Where *'jp'* function is an alias for Postgres' *'jsonb_path_query'* defined in the [init script](dumpdb-init.sql).

To find all the resources from a given namespace:

```sql
select 
       name,
       j2y(_) yaml
from
    resources
where
    namespace = 'echo'
```
Where *'j2y'* function is a python function pre-defined in the [init script](dumpdb-init.sql).

To build it from the git repo base directory:
```bash
> cd dumpdb
> podman build . -t dumpdb:latest
```

## Run psql to extract a series of yaml manifests
```sql
psql -qtAX -P pager=off -c "select '---' || chr(10) || j2y(_) from resources where api_id = 'securitycontextconstraints'"
```
## List non system Namespaces ordered by name
```sql
select distinct namespace 
from resources 
where 
namespace is not null 
and 
namespace not in (
	select distinct namespace 
	from resources 
	where 
	namespace like any (array['openshift-%','kube-%','default'])) 
order by 1;
```

Have fun!