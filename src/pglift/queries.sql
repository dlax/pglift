-- name: role_exists
SELECT true FROM pg_roles WHERE rolname = %(username)s;

-- name: role_create
CREATE ROLE {username} {options};

-- name: role_has_password
SELECT
    rolpassword IS NOT NULL FROM pg_authid
WHERE
    rolname = %(username)s;

-- name: role_alter
ALTER ROLE {username} {options};

-- name: role_alter_password
ALTER ROLE {username} PASSWORD %(password)s;

-- name: role_inspect
SELECT
    CASE WHEN r.rolpassword IS NOT NULL THEN
        '<set>'
    ELSE
        NULL
    END AS password,
    r.rolinherit AS inherit,
    r.rolcanlogin AS login,
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
    r.rolpassword, r.rolinherit, r.rolcanlogin, r.rolconnlimit, r.rolvaliduntil;

-- name: role_grant
GRANT {rolname} TO {rolspec};

-- name: role_revoke
REVOKE {rolname} FROM {rolspec};

-- name: role_drop
DROP ROLE {username};

-- name: database_exists
SELECT true FROM pg_database WHERE datname = %(database)s;

-- name: database_create
CREATE DATABASE {database} {options};

-- name: database_alter_owner
ALTER DATABASE {database} {options};

-- name: database_inspect
SELECT
    r.rolname AS owner
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
    d.datacl AS "acls",
    pg_catalog.pg_database_size(d.datname) as "size",
    t.spcname as "tablespace",
    pg_catalog.pg_tablespace_location(t.oid) as "tablespace_location",
    pg_catalog.pg_tablespace_size(t.oid) as "tablespace_size",
    pg_catalog.shobj_description(d.oid, 'pg_database') as "description"
FROM pg_catalog.pg_database d
JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid
WHERE datallowconn
ORDER BY 1;

-- name: database_drop
DROP DATABASE {database};

-- name: drop_replication_slot
SELECT true FROM pg_drop_replication_slot((SELECT slot_name FROM pg_replication_slots WHERE slot_name = %(slot)s));

-- name: create_replication_slot
SELECT true FROM pg_create_physical_replication_slot(%(slot)s);
