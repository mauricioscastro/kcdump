create extension if not exists plpython3u;

create or replace function jp(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns setof jsonb
    language sql
    immutable strict parallel safe as
'select jsonb_path_query(target, path, vars, silent)';

create or replace function jpone(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns jsonb
    language sql
    immutable strict parallel safe as
'select jsonb_path_query(target, path, vars, silent) limit 1';

create or replace function jptxt(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns setof text
    language sql
    immutable strict parallel safe as
'select replace(jsonb_path_query(target, path, vars, silent) #>> ''{}'', ''{}'', '''')';

create or replace function jptxtone(target jsonb, path jsonpath, vars jsonb default '{}', silent boolean default true)
    returns text
    language sql
    immutable strict parallel safe as
'select replace(jsonb_path_query(target, path, vars, silent) #>> ''{}'', ''{}'', '''') limit 1';

create or replace function jsonb_array_to_text_array(_js jsonb)
    returns text[]
    language sql
    immutable strict parallel safe as
'select array(select jsonb_array_elements_text(_js))';

create or replace function toyaml(js jsonb) returns text as
$$
  import json
  import yaml
  global js
  if js == None or js == "{}": return ""
  return yaml.dump(json.loads(js))
$$ language plpython3u;

create or replace function clean_views()
    returns void
    language plpgsql as
$$
declare
    v record;
begin
    for v in
       select matviewname from pg_matviews
    loop
        execute format('drop materialized view %s', v.matviewname);
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
  for f in glob.iglob(dir+"/**/*.json.gz", recursive = True):
    cluster.append([os.path.basename(f).replace(".json.gz",""), f])
  return cluster
$$ language plpython3u;

create or replace function load_cluster_data(dir text)
    returns void
    language plpgsql as
$$
declare
    cdata cluster_data;
    apir record;
begin
    perform clean_views();
    drop table if exists cluster;

    create table if not exists cluster
    (
        id       text,
        api_name text,
        api_gv   text,
        api_k    text,
        _        jsonb
    );

    --
    -- load kcdump files into cluster table
    --
    for cdata in
        select * from ls_cluster_data(dir)
    loop
        execute format('copy cluster (_) from program ''gzip -dc %s'';', cdata.data_file);

        update cluster set id = cdata.cluster,
                           api_name = 'apiresources',
                           api_gv = _ ->> 'apiVersion',
                           api_k = replace(_ ->> 'kind', 'List', '')
        where _ ->> 'kind' = 'APIResourceList' and id is null;

        update cluster set id = cdata.cluster,
                           api_name = 'versions',
                           api_gv = 'v1',
                           api_k = 'Version'
        where _ ?& array['buildDate', 'dumpDate'] and id is null;

        update cluster set id = cdata.cluster,
                           api_name = jptxtone(_,'$.items[0].metadata.annotations.apiResourceName'),
                           api_gv = _ ->> 'apiVersion',
                           api_k = replace(_ ->> 'kind', 'List', '')
        where api_name is null and id is null;
    end loop;

    create index if not exists cluster_id_gv_name on cluster (id, api_name, api_gv);
    create index if not exists cluster_k on cluster (api_k);

    --
    -- create basic api_resources and version views
    --
    create materialized view if not exists api_resources as
    select id                                                            cluster_id,
           _ ->> 'name'                                                  api_name,
           _ ->> 'groupVersion'                                          api_gv,
           _ ->> 'kind'                                                  api_k,
           _ ->> 'namespaced'                                            namespaced,
           jsonb_array_to_text_array(jp(_, '$.shortNames')) short_names,
           jsonb_array_to_text_array(jp(_, '$.verbs'))      verbs
    from (select jp(_, '$.items[*]') _, id from cluster where api_name = 'apiresources');

    create index if not exists apiresources_clnamegv on api_resources (cluster_id, api_name, api_gv);
    create index if not exists apiresources_k on api_resources (api_k);

    create materialized view if not exists version as
    select
        id                      cluster_id,
        _ ->> 'dumpDate'        dump_date,
        _ ->> 'major'           major,
        _ ->> 'minor'           minor,
        _ ->> 'compiler'        compiler,
        _ ->> 'platform'        platform,
        _ ->> 'buildDate'       build_date,
        _ ->> 'goVersion'       go_version,
        _ ->> 'gitCommit'       git_commit,
        _ ->> 'gitVersion'      git_version,
        _ ->> 'gitTreeState'    git_tree_state
    from  cluster where api_name = 'versions' and api_k = 'Version';

    --
    -- create views for all api resources with at least one object found for all clusters
    --
    for apir in
        select distinct
            a.api_name, a.api_gv, a.namespaced, replace(replace(replace(a.api_gv, '-', '_'), '/', '_'),'.','_') gvname
        from
            api_resources a join cluster c on a.api_name = c.api_name and a.api_gv = c.api_gv
    loop
        if apir.namespaced then
            execute format('
            create materialized view if not exists %s_%s as
            select c.id cluster_id, c.api_name, c.api_gv gv, c.api_k kind,
                   jp(c._, ''$.items[*].metadata.name'')->>0 name,
                   jp(c._, ''$.items[*].metadata.namespace'')->>0 namespace,
                   jp(c._, ''$.items[*]'') _
                   from cluster c
            where c.api_name=''%s'' and c.api_gv=''%s'';
            ', apir.api_name, apir.gvname, apir.api_name, apir.api_gv);
            execute format ('create index if not exists %s_clapinamegv on %s_%s (cluster_id, api_name, gv);', apir.api_name, apir.api_name, apir.gvname);
            execute format ('create index if not exists %s_namens on %s_%s (name, namespace);', apir.api_name, apir.api_name, apir.gvname);
            execute format ('create index if not exists %s_k on %s_%s (kind);', apir.api_name, apir.api_name, apir.gvname);
        else
            execute format('
            create materialized view if not exists %s_%s as
            select c.id cluster_id, c.api_name, c.api_gv gv, c.api_k kind,
                   jp(c._, ''$.items[*].metadata.name'')->>0 name,
                   jp(c._, ''$.items[*]'') _
                   from cluster c
            where c.api_name=''%s'' and c.api_gv=''%s'';
            ', apir.api_name, apir.gvname, apir.api_name, apir.api_gv);
            execute format ('create index if not exists %s_clapinamegv on %s_%s (cluster_id, api_name, gv);', apir.api_name, apir.api_name, apir.gvname);
            execute format ('create index if not exists %s_name on %s_%s (name);', apir.api_name, apir.api_name, apir.gvname);
            execute format ('create index if not exists %s_k on %s_%s (kind);', apir.api_name, apir.api_name, apir.gvname);
        end if;
    end loop;
end;
$$;

-- select load_cluster_data('/kcdump');