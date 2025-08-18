CREATE EXTENSION IF NOT EXISTS plpython3u;

CREATE OR REPLACE FUNCTION j2y(json_data jsonb)
RETURNS text
LANGUAGE plpython3u
AS $$
import json
import yaml

try:
    if json_data is None:
        return None
    
    return yaml.dump(json.loads(json_data), default_flow_style=False, sort_keys=False, allow_unicode=True)
    
except Exception as e:
    plpy.error(f"Error converting JSON to YAML: {str(e)}")
$$;

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

create or replace function load_cluster_data(dir text)
returns void
language plpgsql as
$$
declare
    cdata record;
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
		api_id	 text,
        _        jsonb
    );

    --
    -- load kcdump files into cluster table
    --
    for cdata in
        select replace(pg_ls_dir, '.json.gz', '') cluster, dir || '/' || pg_ls_dir data_file from (
        select * from pg_ls_dir(dir) where pg_ls_dir like '%.json.gz')
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
                           api_name = _ #>> '{metadata,apiName}',
                           api_gv = _ ->> 'apiVersion',
                           api_k = replace(_ ->> 'kind', 'List', '')
        where api_name is null and id is null;
    end loop;

    create index if not exists cluster_id_gv_name on cluster (id, api_name, api_gv);
    create index if not exists cluster_k on cluster (api_k);

    --
    -- create basic apiresources table and versions view
    --
    drop table if exists apiresources;
    create table apiresources as 
        select 
            c.id cluster_id, 
            '' api_id,
            a.*
            from cluster c, json_table (_, '$.items[*]' 
            columns (
                api_name text path '$.name',
                api_gv text path '$.groupVersion',
                api_k text path '$.kind',
                namespaced bool path '$.namespaced',
                short_names text[] path '$.shortNames' default '{}'::text[] on empty,
                verbs text[] path '$.verbs' default '{}'::text[] on empty
            )
        ) a where c.api_name = 'apiresources';

    update apiresources set api_id = lower(api_name) where api_gv not like '%.%' and api_name in 
    ( 
    select api_name from 
        ( 
        select count(api_name) t, api_name, api_id from apiresources where api_id = '' group by api_name, api_id
        ) where t = 1
    );

    update apiresources set api_id = lower(api_name) where api_name in 
    ( 
    select api_name from 
        ( 
        select count(api_name) t, api_name, api_id from apiresources where api_id = '' group by api_name, api_id
        ) where t = 1
    );

    update apiresources set api_id = lower(api_name) where api_gv not like '%.%' and api_name in 
    ( 
    select api_name from 
        ( 
        select count(api_name) t, api_name, api_id from apiresources where api_id = '' group by api_name, api_id
        ) where t > 1
    );

    update apiresources set api_id = lower(api_name) || '_' || REGEXP_REPLACE(api_gv, '^.*\/', '') where api_name in 
    (
    select api_name from (
        select count(api_name) t, api_name, api_grp from (
            select api_id, api_name, REGEXP_REPLACE(api_gv, '\/.*$', '') api_grp from apiresources where api_id = ''
            ) group by api_name, api_grp 
        ) where t > 1
    );

    update apiresources set api_id = lower(api_name) || '_' || REGEXP_REPLACE(api_gv, '^([^.]+)\.([^.]+)\.?([^.]+)?.*\/.*$', '\1_\2') where (api_name, api_gv) in
    (
        select api_name, api_gv from (
            select count(api_name) t, api_name, api_grp, api_gv from (
                select api_id, api_name, REGEXP_REPLACE(api_gv, '^([^.]+)\.([^.]+)\.?([^.]+)?.*\/.*$', '\1_\2') api_grp, api_gv from apiresources where api_id = ''
            ) group by api_name, api_grp, api_gv 
        ) where t = 1
    );

    update apiresources set api_id = REGEXP_REPLACE(replace(api_gv, '.','_'), '\/.*$','') where api_id = '';

    create index if not exists apiresources_clnamegv on apiresources (cluster_id, api_name, api_gv);
    create index if not exists apiresources_k on apiresources (api_k);
    create index if not exists apiresources_pk on apiresources (api_id);

	update cluster c set api_id = a.api_id 
	from apiresources a 
	where c.api_name = a.api_name and c.api_gv = a.api_gv;

	update cluster set api_id = 'apiresources' where api_name = 'apiresources' and api_gv = 'v1';
	update cluster set api_id = 'versions' where api_name = 'versions' and api_gv = 'v1';

	create index if not exists cluster_api_id on cluster (api_id);

    create materialized view if not exists versions as
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
    -- exploded resources view
    --
    create materialized view if not exists resources as
    select * from (
        select 
            id cluster_id,
            api_id, 
            api_gv gv, 
            api_k kind,
            jp(_, '$.items[*].metadata.name')->>0 name,
            jp(_, '$.items[*].metadata.namespace')->>0 namespace,
            jp(_, '$.items[*]') _
        from cluster where api_id not in ('apiresources', 'versions', 'namespaces')
    );

    create index if not exists resources_cluster_id on resources (cluster_id);
    create index if not exists resources_api_id on resources (api_id);
    create index if not exists resources_name on resources (name);
    create index if not exists resources_namespace on resources (namespace);
    create index if not exists resources_kind on resources (kind);

    --
    -- create views for all api resources with at least one object found for all clusters
    --
    -- for apir in
    --     select a.api_id, a.namespaced from apiresources a join cluster c on a.api_id = c.api_id
    -- loop
    --     if apir.namespaced then
    --         execute format('
    --         create materialized view if not exists %s as
    --         select c.id cluster_id, c.api_name, c.api_gv gv, c.api_k kind,
    --                jp(c._, ''$.items[*].metadata.name'')->>0 name,
    --                jp(c._, ''$.items[*].metadata.namespace'')->>0 namespace,
    --                jp(c._, ''$.items[*]'') _
    --                from cluster c
    --         where c.api_id=''%s'';
    --         ', apir.api_id, apir.api_id);
    --         execute format ('create index if not exists %s_clapinamegv on %s (cluster_id, api_name, gv);', apir.api_id, apir.api_id);
    --         execute format ('create index if not exists %s_namens on %s (name, namespace);',apir.api_id, apir.api_id);
    --         execute format ('create index if not exists %s_k on %s (kind);', apir.api_id, apir.api_id);
    --     else
    --         execute format('
    --         create materialized view if not exists %s as
    --         select c.id cluster_id, c.api_name, c.api_gv gv, c.api_k kind,
    --                jp(c._, ''$.items[*].metadata.name'')->>0 name,
    --                jp(c._, ''$.items[*]'') _
    --                from cluster c
    --         where c.api_id=''%s'';
    --         ', apir.api_id, apir.api_id);
    --         execute format ('create index if not exists %s_clapinamegv on %s (cluster_id, api_name, gv);', apir.api_id, apir.api_id);
    --         execute format ('create index if not exists %s_name on %s (name);', apir.api_id, apir.api_id);
    --         execute format ('create index if not exists %s_k on %s (kind);', apir.api_id, apir.api_id);
    --     end if;
    -- end loop;
end;
$$;



-- select load_cluster_data('/kcdump');