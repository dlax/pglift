#!/bin/sh
#
# Functional tests for Ansible playbooks in the docs/ansible (and the
# postgresql_instance Ansible module).
#

set -e

cleanup () (
    set +e
    unset -v SETTINGS
    python -m pglift.install --uninstall
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
python -m pglift.install --settings="$settings_path"

postgresql_surole_password=s3kret
export postgresql_surole_password
export PGPASSFILE=$passfile

query="select setting from pg_settings where name = 'cluster_name';"
list_timers () (
    if type systemctl > /dev/null;
    then
        systemctl --no-pager --user list-timers
    fi
)

set -x

ansible-playbook -vvv --module-path=ansible/modules/  docs/ansible/play1.yml
cat "$passfile"

psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
set +e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5444"  # dev
set -e
list_timers

ansible-playbook -vvv --module-path=ansible/modules/  docs/ansible/play2.yml
cat "$passfile"

psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
set +e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
set -e
psql -w -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5455"  # dev
list_timers

ansible-playbook -vvv --module-path=ansible/modules/  docs/ansible/play3.yml
cat "$passfile"
list_timers

ps xf
