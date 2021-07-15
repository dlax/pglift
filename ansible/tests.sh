#!/bin/sh
#
# Functional tests for Ansible playbooks in the docs/ansible (and pglift
# Ansible modules).
#
set -e

cleanup () (
    set +e
    ansible-playbook --module-path=ansible/modules/  docs/ansible/play3.yml
    unset -v SETTINGS
    pglift site-configure uninstall
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
pglift site-configure install --settings="$settings_path"

postgresql_surole_password=s3kret
export postgresql_surole_password
export PGPASSFILE=$passfile

export ANSIBLE_COLLECTIONS_PATHS="./ansible/"

query="select setting from pg_settings where name = 'cluster_name';"
list_timers() {
    if type systemctl > /dev/null;
    then
        systemctl --no-pager --user list-timers
    fi
}
check_postgres_exporter() {
    if type systemctl > /dev/null;
    then
        curl -f -I "http://localhost:$1/metrics"
    fi
}

set -x

ansible-playbook docs/ansible/play1.yml
cat "$passfile"

psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
check_postgres_exporter 9187
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
check_postgres_exporter 9188
set +e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5444"  # dev
check_postgres_exporter 9189
set -e
list_timers

ansible-playbook docs/ansible/play2.yml
cat "$passfile"
grep bob "$passfile"

psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
check_postgres_exporter 9187
set +e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
check_postgres_exporter 9188
set -e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5455"  # dev
check_postgres_exporter 9189
list_timers

ansible-playbook docs/ansible/play3.yml
if test -f "$passfile";
then
    echo "password file $passfile still exists"
    exit 1
fi
list_timers

ps xf
