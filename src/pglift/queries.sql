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
CREATE DATABASE {database};

-- name: database_drop
DROP DATABASE {database};
