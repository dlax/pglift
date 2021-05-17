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
cat > "$settings_path" << EOF
{
  "prefix": "$tmpdir",
  "postgresql": {
    "root": "$tmpdir/postgresql"
  }
}
EOF
export SETTINGS="@$settings_path"
python -m pglift.install --settings="$settings_path"

query="select setting from pg_settings where name = 'cluster_name';"
list_timers () (
    if type systemctl > /dev/null;
    then
        systemctl --no-pager --user list-timers
    fi
)

set -x

ansible-playbook --module-path=ansible/modules/  docs/ansible/play1.yml

psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
set +e
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5444"  # dev
set -e
list_timers

ansible-playbook --module-path=ansible/modules/  docs/ansible/play2.yml

psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
set +e
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
set -e
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5455"  # dev
list_timers

ansible-playbook --module-path=ansible/modules/  docs/ansible/play3.yml
list_timers

ps xf
