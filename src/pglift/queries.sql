-- name: role_exists
SELECT true FROM pg_roles WHERE rolname = %(username)s

-- name: role_create
CREATE ROLE {username} PASSWORD %(password)s

-- name: role_create_no_password
CREATE ROLE {username}

-- name: role_alter_password
ALTER ROLE {username} PASSWORD %(password)s
