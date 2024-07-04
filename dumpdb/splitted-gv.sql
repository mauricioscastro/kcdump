create extension if not exists plpython3u;

create schema if not exists hcr;
set search_path to hcr;

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

create or replace function load_api_resources_and_version_tables(dir text)
  returns void
  language plpgsql as
$$
declare
    i record;
begin
    for i in
        select table_name, file_name
        from
            ls_json_in_dir(dir)
        where
            file_name like '%api_resources.json' or
            file_name like '%version.json'
    loop
        execute format ('copy %s (_) from ''%s'';', i.table_name, i.file_name);
    end loop;
end;
$$;

create or replace function load_all_tables(dir text)
    returns void
    language plpgsql as
$$
declare
    i record;
begin
    for i in
        select t.name,
               t.gv,
               a.k,
               t.file_name,
               t.table_name,
               a.namespaced::bool
        from ls_json_in_dir(dir) t
                 inner join api_resources_view a on t.gv = a.gv and t.name = a.name
        where table_name not in ('api_resources', 'version')
    loop
        if i.namespaced then
            execute format(
            'drop table if exists %s;
            create table if not exists %s
            (
                group_version     text,
                kind              text,
                api_resource_name text,
                name              text,
                namespace         text,
                _                 jsonb
            );',
            i.table_name, i.table_name);
        else
            execute format(
            'drop table if exists %s;
            create table if not exists %s
            (
                group_version     text,
                kind              text,
                api_resource_name text,
                name              text,
                _                 jsonb
            );',
            i.table_name, i.table_name);
        end if;
        execute format('copy %s (_) from ''%s'';',
                       i.table_name, i.file_name);
        execute format('update %s set (group_version, kind, api_resource_name) = (''%s'', ''%s'', ''%s'');',
                        i.table_name, i.gv, i.k, i.name);
        if i.namespaced then
            execute format('update %s set name = jptxtone(_,''$.metadata.name''), namespace = jptxtone(_,''$.metadata.namespace'');',
                           i.table_name);
            execute format('create index if not exists idx_%s_namens on %s (name, namespace);',
                           i.name, i.table_name);
        else
            execute format('update %s set name = jptxtone(_,''$.metadata.name'');',
                           i.table_name);
            execute format('create index if not exists idx_%s_name on %s (name);',
                           i.name, i.table_name);

        end if;
    end loop;
end;
$$;

drop function if exists ls_json_in_dir;
drop type if exists api_resource;
create type api_resource as
(
    file_name  text,
    table_name text,
    name       text,
    gv         text
);

create or replace function ls_json_in_dir(dir text) returns setof api_resource as
$$
  import os
  import glob
  global dir
  suffix = '.json'
  apir = []
  for i in glob.iglob(dir+"/**/*.json", recursive = True):
    sp = os.path.basename(i).split('.')
    splitted_gv = sp[1].split('_')
    gvjchar = '/' if len(splitted_gv) > 1 else ''
    gv = ".".join(splitted_gv[:-1]) + gvjchar + splitted_gv[-1] if len(sp) > 2 else ''
    table = sp[0]+'_'+sp[1].replace("-","_") if len(sp) > 2 else sp[0]
    apir.append([i, table, sp[0], gv])
  return apir
$$ language plpython3u;

create or replace function toyaml(js jsonb) returns text as
$$
  import json
  import yaml
  global js
  return yaml.dump(json.loads(js))
$$ language plpython3u;

drop materialized view if exists api_resources_view;
drop table if exists api_resources;
create table if not exists api_resources (_  jsonb);
drop table if exists version;
create table if not exists version (_  jsonb);

select load_api_resources_and_version_tables('/kcdump');

create materialized view if not exists api_resources_view as
select _ ->> 'groupVersion'                                           gv,
       _ ->> 'kind'                                                   k,
       _ ->> 'name'                                                   name,
       _ ->> 'namespaced'                                             namespaced,
       jsonb_array_to_text_array(jp(_, '$.shortNames')) short_names,
       jsonb_array_to_text_array(jp(_, '$.verbs'))      verbs
from api_resources;

create index if not exists idx_api_resources_view_gv_name on api_resources_view (gv, name);
create index if not exists idx_api_resources_view_k on api_resources_view (k);

create materialized view if not exists version_view as
select
    _ ->> 'major' major,
    _ ->> 'minor' minor,
    _ ->> 'compiler' compiler,
    _ ->> 'platform' platform,
    _ ->> 'buildDate' buildDate,
    _ ->> 'gitCommit' gitCommit,
    _ ->> 'goVersion' goVersion,
    _ ->> 'gitVersion' gitVersion,
    _ ->> 'gitTreeState' gitTreeState
from version;

select load_all_tables('/kcdump');

