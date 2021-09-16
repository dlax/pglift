#!/bin/sh
#
# Functional tests for Ansible playbooks in the docs/ansible (and pglift
# Ansible modules).
#
set -e

cleanup () (
    set +e
    ansible-playbook --module-path=ansible/modules/  docs/ansible/play3.yml
    ansible-playbook --module-path=ansible/modules/ "$tmpdir/play-exporter-dropped.yml"
    unset -v SETTINGS
    rm -rf "$tmpdir"
)
trap cleanup EXIT INT

tmpdir=$(mktemp -d)

echo "Working in $tmpdir"

settings_path=$tmpdir/config.json
passfile=$tmpdir/pgpass
cat > "$settings_path" << EOF
{
  "prefix": "$tmpdir",
  "postgresql": {
    "auth": {
      "local": "md5",
      "passfile": "$passfile"
    },
    "surole": {
      "pgpass": true
    },
    "root": "$tmpdir/postgresql"
  }
}
EOF
export SETTINGS="@$settings_path"

postgresql_surole_password=s3kret
export postgresql_surole_password
export PGPASSFILE=$passfile

cat > "$tmpdir/play-exporter.yml" << EOF
- name: unbound postgres_exporter
  hosts: localhost
  tasks:
    - name: second postgres_exporter for prod
      dalibo.pglift.postgres_exporter:
        name: prod2
        dsn: "port=5433 host=/tmp user=postgres password=$postgresql_surole_password"
        port: 9191
EOF
cat > "$tmpdir/play-exporter-dropped.yml" << EOF
- name: unbound postgres_exporter (dropped)
  hosts: localhost
  tasks:
    - name: second postgres_exporter for prod (dropped)
      dalibo.pglift.postgres_exporter:
        name: prod2
        dsn: "port=5433 host=/tmp user=postgres password=$postgresql_surole_password"
        port: 9191
        state: absent
EOF

export ANSIBLE_COLLECTIONS_PATHS="./ansible/"

query="select setting from pg_settings where name = 'cluster_name';"
list_timers() {
    if type systemctl > /dev/null;
    then
        systemctl --no-pager --user list-timers
    fi
}
check_postgres_exporter() {
    nc -v -w1 -z localhost "$1"
}

set -x

ansible-playbook docs/ansible/play1.yml
cat "$passfile"
grep -q bob "$passfile"

ps xf

psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
psql -w -t -e -c "select rolname,rolinherit,rolcanlogin,rolconnlimit,rolpassword,rolvaliduntil from pg_roles where rolname = 'bob';" "host=/tmp user=postgres dbname=postgres port=5433"
psql -w -t -e -c "SELECT r.rolname AS role, ARRAY_AGG(m.rolname) AS member_of FROM pg_auth_members JOIN pg_authid m ON pg_auth_members.roleid = m.oid JOIN pg_authid r ON pg_auth_members.member = r.oid GROUP BY r.rolname" "host=/tmp user=postgres dbname=postgres port=5433"
check_postgres_exporter 9186
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
check_postgres_exporter 9188
set +e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5444"  # dev
check_postgres_exporter 9189
set -e
list_timers

ansible-playbook docs/ansible/play2.yml
cat "$passfile"
if grep -q bob "$passfile";
then
    echo "'bob' user still present in password file $passfile"
    exit 1
fi

ps xf

psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
check_postgres_exporter 9186
set +e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
check_postgres_exporter 9188
set -e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5455"  # dev
check_postgres_exporter 9189
list_timers

ansible-playbook "$tmpdir/play-exporter.yml"
check_postgres_exporter 9191

ansible-playbook docs/ansible/play3.yml
if test -f "$passfile";
then
    echo "password file $passfile still exists"
    exit 1
fi
list_timers

ps xf

# vim: tw=0
