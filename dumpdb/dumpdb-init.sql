create extension if not exists plpython3u;

create schema if not exists f;
set search_path to f;

create or replace function jp(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns setof jsonb
    language sql
    immutable strict parallel safe as
'select jsonb_path_query(target, path, vars, silent)';

create or replace function jptxt(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns setof text
    language sql
    immutable strict parallel safe as
'select jsonb_path_query(target, path, vars, silent) #>> ''{}''';

create or replace function jptxtone(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns text
    language sql
    immutable strict parallel safe as
'select jsonb_path_query(target, path, vars, silent) #>> ''{}'' limit 1';

create or replace function jsonb_array_to_text_array(_js jsonb)
    returns text[]
    language sql
    immutable strict parallel safe as
'select array(select jsonb_array_elements_text(_js))';

create or replace function clean_views()
    returns void
    language plpgsql as
$$
declare
    v record;
begin
    for v in
       select schemaname, matviewname from pg_matviews where matviewname not in ('api_resources', 'version')
    loop
        execute format('drop materialized view %s.%s', v.schemaname, v.matviewname);
    end loop;
end;
$$;

drop function if exists ls_cluster_data;
drop type if exists cluster_data;

create type cluster_data as
(
    cluster  text,
    data_file text
);

create or replace function ls_cluster_data(dir text) returns setof cluster_data as
$$
  import os
  import glob
  global dir
  cluster = []
  for p in ['.json.gz', '.json']:
    for f in glob.iglob(dir+"/**/*"+p, recursive = True):
        cluster.append([os.path.basename(f).replace(p,""), f])
  return cluster
$$ language plpython3u;

create or replace function load_cluster_data(dir text)
    returns void
    language plpgsql as
$$
declare
    cdata f.cluster_data;
    apir record;
begin
    perform f.clean_views();
    for cdata in
        select * from f.ls_cluster_data(dir)
    loop
        execute format('create schema if not exists %s;', cdata.cluster);
        execute format('set search_path to %s;', cdata.cluster);

        drop materialized view if exists api_resources;
        drop materialized view if exists version;
        drop table if exists cluster;

        create table if not exists cluster
        (
            name text,
            gv   text,
            k    text,
            _    jsonb
        );

        execute format('copy cluster (_) from program ''gzip -dc %s'';', cdata.data_file);

        update cluster set name = 'apiresources',
                           gv = _ ->> 'apiVersion',
                           k = replace(_ ->> 'kind', 'List', '')
        where _ ->> 'kind' = 'APIResourceList';

        update cluster set name = 'version',
                           gv = 'v1',
                           k = 'Version'
        where _ ?& array['buildDate', 'hcrDate'];

        update cluster set name = f.jptxtone(_,'$.items[0].metadata.annotations.apiResourceName'),
                           gv = _ ->> 'apiVersion',
                           k = replace(_ ->> 'kind', 'List', '')
        where name is null;

        create index if not exists cluster_gv_name on cluster (name, gv);
        create index if not exists cluster_k on cluster (k);

        create materialized view if not exists api_resources as
        select _ ->> 'name'                                     name,
               _ ->> 'groupVersion'                             gv,
               _ ->> 'kind'                                     k,
               _ ->> 'namespaced'                               namespaced,
               f.jsonb_array_to_text_array(f.jp(_, '$.shortNames')) short_names,
               f.jsonb_array_to_text_array(f.jp(_, '$.verbs'))      verbs
        from (select f.jp(_, '$.items[*]') _ from cluster where name = 'apiresources');

        create materialized view if not exists version as
        select
            _ ->> 'hcrDate' hcr_date,
            _ ->> 'major' major,
            _ ->> 'minor' minor,
            _ ->> 'compiler' compiler,
            _ ->> 'platform' platform,
            _ ->> 'buildDate' build_date,
            _ ->> 'goVersion' go_version,
            _ ->> 'gitCommit' git_commit,
            _ ->> 'gitVersion' git_version,
            _ ->> 'gitTreeState' git_tree_state
        from  cluster where name = 'version' and k = 'Version';

        for apir in
            select a.name, a.gv, a.namespaced,  replace(replace(replace(a.gv, '-', '_'), '/', '_'),'.','_') gvname from api_resources a join cluster c on a.name = c.name and a.gv = c.gv
        loop
            if apir.namespaced then
                execute format('
                create materialized view if not exists %s_%s as
                select a.name api_resource_name, a.gv, a.k kind,
                       f.jp(c._, ''$.items[*].metadata.name'')->>0 name,
                       f.jp(c._, ''$.items[*].metadata.namespace'')->>0 namespace,
                       f.jp(c._, ''$.items[*]'') _
                       from api_resources a
                join cluster c on a.name = c.name and a.gv = c.gv
                where a.name=''%s'' and a.gv=''%s'';
                ', apir.name, apir.gvname, apir.name, apir.gv);
                execute format ('create index if not exists %s_apinamegv on %s_%s (api_resource_name, gv);', apir.name, apir.name, apir.gvname);
                execute format ('create index if not exists %s_namens on %s_%s (name, namespace);', apir.name, apir.name, apir.gvname);
                execute format ('create index if not exists %s_k on %s_%s (kind);', apir.name, apir.name, apir.gvname);
            else
                execute format('drop materialized view if exists %s_%s', apir.name, apir.gvname);
                execute format('
                create materialized view if not exists %s_%s as
                select a.name api_resource_name, a.gv, a.k kind,
                       f.jp(c._, ''$.items[*].metadata.name'')->>0 name,
                       f.jp(c._, ''$.items[*]'') _
                       from api_resources a
                join cluster c on a.name = c.name and a.gv = c.gv
                where a.name=''%s'' and a.gv=''%s'';
                ', apir.name, apir.gvname, apir.name, apir.gv);
                execute format ('create index if not exists %s_apinamegv on %s_%s (api_resource_name, gv);', apir.name, apir.name, apir.gvname);
                execute format ('create index if not exists %s_name on %s_%s (name);', apir.name, apir.name, apir.gvname);
                execute format ('create index if not exists %s_k on %s_%s (kind);', apir.name, apir.name, apir.gvname);
            end if;
        end loop;
    end loop;
end;
$$;

-- select f.load_cluster_data('/kcdump');

