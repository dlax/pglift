#!/bin/sh
#
# Functional tests for Ansible playbooks in the docs/ansible (and the
# postgresql_instance Ansible module).
#

set -e

tmpdir=$(mktemp -d)

echo "Working in $tmpdir"
trap 'rm -rf $tmpdir' EXIT

export SETTINGS=$tmpdir/config.json
cat > "$SETTINGS" << EOF
{
  "postgresql": {
    "root": "$tmpdir/postgresql"
  },
  "pgbackrest": {
    "configpath": "$tmpdir/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf",
    "directory": "$tmpdir/var/lib/pgbackrest/{instance.version}-{instance.name}",
    "logpath": "$tmpdir/var/lib/pgbackrest/{instance.version}-{instance.name}/logs"
  },
  "prometheus": {
    "configpath": "$tmpdir/etc/prometheus/postgres_exporter-{instance.version}-{instance.name}.conf",
    "queriespath": "$tmpdir/etc/prometheus/postgres_exporter_queries-{instance.version}-{instance.name}.yaml"
  }
}
EOF

query="select setting from pg_settings where name = 'cluster_name';"

set -x

ansible-playbook --module-path=ansible/modules/  docs/ansible/play1.yml

psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
set +e
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5444"  # dev
set -e
crontab -l

ansible-playbook --module-path=ansible/modules/  docs/ansible/play2.yml

psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5433"  # prod
set +e
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5434"  # preprod
set -e
psql -t -e -c "$query" "host=/tmp user=postgres dbname=postgres port=5455"  # dev
crontab -l

ansible-playbook --module-path=ansible/modules/  docs/ansible/play3.yml

crontab -l
ps xf
