-- name: role_exists
SELECT true FROM pg_roles WHERE rolname = %(username)s;

-- name: role_create
CREATE ROLE {username} {options};

-- name: role_has_password
SELECT
    rolpassword IS NOT NULL as haspassword FROM pg_authid
WHERE
    rolname = %(username)s;

-- name: role_alter
ALTER ROLE {username} {options};

-- name: role_inspect
SELECT
    r.rolname AS name,
    CASE WHEN r.rolpassword IS NOT NULL THEN
        '<set>'
    ELSE
        NULL
    END AS password,
    r.rolinherit AS inherit,
    r.rolcanlogin AS login,
    r.rolsuper AS superuser,
    r.rolreplication AS replication,
    CASE WHEN r.rolconnlimit <> - 1 THEN
        r.rolconnlimit
    ELSE
        NULL
    END AS connection_limit,
    r.rolvaliduntil AS validity,
    CASE WHEN COUNT(gi) <> 0 THEN
        ARRAY_AGG(gi.rolname)
    ELSE
        ARRAY[]::text[]
    END AS in_roles
FROM
    pg_authid r
    LEFT OUTER JOIN pg_auth_members g ON g.member = r.oid
    LEFT OUTER JOIN pg_authid gi ON g.roleid = gi.oid
WHERE
    r.rolname = %(username)s
GROUP BY
    r.rolname,
    r.rolpassword,
    r.rolinherit,
    r.rolcanlogin,
    r.rolsuper,
    r.rolreplication,
    r.rolconnlimit,
    r.rolvaliduntil;

-- name: role_grant
GRANT {rolname} TO {rolspec};

-- name: role_revoke
REVOKE {rolname} FROM {rolspec};

-- name: role_list_names
SELECT rolname from pg_roles ORDER BY rolname;

-- name: role_drop
DROP ROLE {username};

-- name: database_exists
SELECT true FROM pg_database WHERE datname = %(database)s;

-- name: database_create
CREATE DATABASE {database} {options};

-- name: database_alter
ALTER DATABASE {database} {options};

-- name: database_inspect
SELECT
    db.datname AS name,
    r.rolname AS owner,
    (
        SELECT s.setconfig FROM pg_db_role_setting s
        WHERE s.setdatabase = db.oid AND s.setrole = 0
    ) AS settings
FROM
    pg_database db
    JOIN pg_authid r ON db.datdba = r.oid
WHERE
    db.datname = %(datname)s;

-- name: database_list
SELECT d.datname as "name",
    pg_catalog.pg_get_userbyid(d.datdba) as "owner",
    pg_catalog.pg_encoding_to_char(d.encoding) as "encoding",
    d.datcollate as "collation",
    d.datctype as "ctype",
    coalesce(d.datacl, '{{}}'::aclitem[]) AS "acls",
    pg_catalog.pg_database_size(d.datname) as "size",
    t.spcname as "tablespace",
    pg_catalog.pg_tablespace_location(t.oid) as "tablespace_location",
    pg_catalog.pg_tablespace_size(t.oid) as "tablespace_size",
    pg_catalog.shobj_description(d.oid, 'pg_database') as "description"
FROM pg_catalog.pg_database d
JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
WHERE datallowconn {where_clause}
ORDER BY 1;

-- name: database_drop
DROP DATABASE {database};

-- name: database_default_acl
WITH default_acls AS (
    SELECT
        pg_namespace.nspname AS schema,
        pg_default_acl.defaclobjtype AS objtype,
        aclexplode(pg_default_acl.defaclacl) AS acl
    FROM
        pg_default_acl
        JOIN pg_namespace ON pg_namespace.oid = pg_default_acl.defaclnamespace
)
SELECT
    current_database() AS database,
    default_acls.schema,
    pg_roles.rolname AS role,
    CASE default_acls.objtype
    WHEN 'f' THEN
        'FUNCTION'
    WHEN 'r' THEN
        'TABLE'
    WHEN 'S' THEN
        'SEQUENCE'
    WHEN 'T' THEN
        'TYPE'
    WHEN 'n' THEN
        'SCHEMA'
    ELSE
        'UNKNOWN'
    END AS object_type,
    array_agg(DISTINCT (default_acls.acl).privilege_type) AS privileges
FROM
    default_acls
    JOIN pg_roles ON ((acl).grantee = pg_roles.oid)
{where_clause}
GROUP BY
    schema,
    role,
    object_type
ORDER BY
    schema,
    role,
    object_type;

-- name: database_privileges
WITH relacl AS (
    SELECT
        c.oid,
        rolname,
        array_agg(relacl.privilege_type) AS relacl
    FROM
        pg_catalog.pg_class c
        CROSS JOIN aclexplode(c.relacl) as relacl
        JOIN pg_roles ON (relacl.grantee = pg_roles.oid)
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    GROUP BY 1, 2
),
attacl AS (
    SELECT
        c.oid,
        attname,
        rolname,
        array_agg(attacl.privilege_type) AS attacl
    FROM
        pg_catalog.pg_class c
        JOIN pg_catalog.pg_attribute ON attrelid = c.oid
        CROSS JOIN aclexplode(pg_catalog.pg_attribute.attacl) as attacl
        JOIN pg_roles ON (attacl.grantee = pg_roles.oid)
    WHERE
        NOT attisdropped
        AND attacl IS NOT NULL
    GROUP BY 1, 2, 3
),
attacl_agg AS (
    SELECT
        oid,
        attacl.rolname,
        json_object_agg(
            attacl.attname,
            attacl.attacl
        ) as attacl
    FROM attacl
    GROUP BY 1, 2
)
SELECT
    current_database() AS database,
    n.nspname AS schema,
    c.relname AS object_name,
    CASE c.relkind
        WHEN 'r' THEN
            'TABLE'
        WHEN 'v' THEN
            'VIEW'
        WHEN 'm' THEN
            'MATERIALIZED VIEW'
        WHEN 'S' THEN
            'SEQUENCE'
        WHEN 'f' THEN
            'FOREIGN TABLE'
        WHEN 'p' THEN
            'PARTITIONED TABLE'
        ELSE
            'UNKNOWN'
        END
    AS object_type,
    pg_roles.rolname AS role,
    coalesce(a.relacl, '{{}}'::text[]) AS privileges,
    coalesce(b.attacl, '{{}}'::json) AS column_privileges
FROM pg_class c
CROSS JOIN pg_roles
LEFT JOIN relacl a ON c.oid = a.oid AND pg_roles.rolname = a.rolname
LEFT JOIN attacl_agg b ON c.oid = b.oid AND pg_roles.rolname = b.rolname
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind IN ('r', 'v', 'm', 'S', 'f', 'p')
    AND n.nspname !~ '^pg_'
    AND pg_table_is_visible(c.oid)
    AND (a.relacl IS NOT NULL OR attacl IS NOT NULL)
    {where_clause};

-- name: drop_replication_slot
SELECT true FROM pg_drop_replication_slot((SELECT slot_name FROM pg_replication_slots WHERE slot_name = %(slot)s));

-- name: create_replication_slot
SELECT true FROM pg_create_physical_replication_slot(%(slot)s);

-- name: instance_encoding
SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname='template1';
