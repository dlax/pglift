Using Ansible modules
=====================

.. highlight:: console

This tutorial illustrates the use of the Ansible modules shipped with pglift:
``dalibo.pglift.instance``, ``dalibo.pglift.database``, ``dalibo.pglift.role``,
``dalibo.pglift.postgres_exporter̀`` and ``dalibo.pglift.dsn_info``. It also
demonstrates how to integrate these modules with other PostgreSQL-related
community modules, namely `community.postgresql`_.

.. note::
   Ansible modules require Python 3 so, depending on the Ansible version being
   used, one may need to configure managed machines to use Python 3 through
   the ``ansible_python_interpreter`` inventory variable or ``-e``
   command-line option.

Setup
-----

In the following we consider two nodes (or machines): the ``control`` node,
where Ansible commands or playbooks will be executed and, the ``managed`` node
where operations should apply.

On the ``control`` node, Ansible needs to be installed and the collection
should be available in path, for instance, if you have a local checkout of
pglift repository:
::

    user@control:~$ export ANSIBLE_COLLECTIONS_PATHS="~/src/pglift/ansible/"

On the ``control`` node, documentation for each module can be obtained by
using ``ansible-doc <modulename>``, e.g.:
::

    user@control:~$ ansible-doc dalibo.pglift.instance
    > DALIBO.PGLIFT.INSTANCE
    (.../ansible/ansible_collections/dalibo/pglift/plugins/modules/instance.py)

    Manage a PostgreSQL server instance

    OPTIONS (= is mandatory):

    [...]

On the ``managed`` node, pglift needs to be :ref:`installed <install>`.

On the ``managed`` node, we configure pglift through :ref:`site settings
<settings>` by defining a writable directory to host PostgreSQL instances,
data and configuration files (we use a temporary directory in this tutorial):
::

    user@managed:~$ tmpdir=$(mktemp -d)
    user@managed:~$ cat > ~/.config/pglift/settings.yaml << EOF
    prefix: $tmpdir
    service_manager: systemd
    scheduler: systemd
    postgresql:
      auth:
        local: md5
        host: md5
      root: $tmpdir/postgres
    pgbackrest:
     directory: $tmpdir/backups
    prometheus:
      execpath: /usr/bin/prometheus-postgres-exporter
    temboard: {}
    EOF

.. note::
   If using `systemd` as service manager and/or scheduler as in above example,
   an extra installation step is needed as documented :ref:`here
   <systemd_install>`.

Back on the ``control`` node, we will define passwords for the `postgres` user
and other roles used in the following playbooks; these will be stored and
encrypted with Ansible vault:

::

    user@control:~$ cat << EOF | ansible-vault encrypt > pglift-vars
    postgresql_surole_password: $(openssl rand -base64 9)
    prod_bob_password: $(openssl rand -base64 9)
    prometheus_role_password: $(openssl rand -base64 9)
    temboard_role_password: $(openssl rand -base64 9)
    EOF

To view actual passwords:

::

    user@control:~$ ansible-vault view pglift-vars

Initial deployment
------------------

The following playbook installs and configures 3 PostgreSQL instances on the
``managed`` node; the first two instances are *started* while the third one is
not:

.. literalinclude:: ../ansible/play1.yml
    :language: yaml
    :caption: play1.yml

.. note::
   The ``hosts`` field in this playbook use ``localhost`` for testing purpose
   and should be adapted to actual ``managed`` node.

Finally, run:

::

    user@control:~$ ansible-playbook --extra-vars pglift-vars --ask-vault-password play1.yml
    PLAY [my postgresql instances] ***************************************************************************

    TASK [Gathering Facts] ***********************************************************************************
    ok: [localhost]

    TASK [production instance] *******************************************************************************
    changed: [localhost]

    TASK [pre-production instance] ***************************************************************************
    changed: [localhost]

    TASK [dev instance, not running at the moment] ***********************************************************
    changed: [localhost]

    PLAY RECAP ***********************************************************************************************
    localhost                  : ok=4    changed=3    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0

We can see our instances installed and running on the ``managed`` node:

::

    user@managed:~$ tree -L 3 $tmpdir/postgres
    /tmp/.../postgres
    └── 13
        ├── dev
        │   ├── data
        │   └── wal
        ├── preprod
        │   ├── data
        │   └── wal
        └── prod
            ├── data
            └── wal
    user@managed:~$ ps xf
    [...]
    26856 ?        Ss     0:00  \_ /usr/lib/postgresql/13/bin/postgres -D /tmp/.../postgres/13/prod/data
    26858 ?        Ss     0:00  |   \_ postgres: prod: checkpointer
    26859 ?        Ss     0:00  |   \_ postgres: prod: background writer
    26860 ?        Ss     0:00  |   \_ postgres: prod: walwriter
    26861 ?        Ss     0:00  |   \_ postgres: prod: autovacuum launcher
    26862 ?        Ss     0:00  |   \_ postgres: prod: stats collector
    26863 ?        Ss     0:00  |   \_ postgres: prod: logical replication launcher
    26912 ?        Ss     0:00  \_ /usr/lib/postgresql/13/bin/postgres -D /tmp/.../postgres/13/preprod/data
    26914 ?        Ss     0:00      \_ postgres: preprod: checkpointer
    26915 ?        Ss     0:00      \_ postgres: preprod: background writer
    26916 ?        Ss     0:00      \_ postgres: preprod: walwriter
    26917 ?        Ss     0:00      \_ postgres: preprod: autovacuum launcher
    26918 ?        Ss     0:00      \_ postgres: preprod: stats collector
    26919 ?        Ss     0:00      \_ postgres: preprod: logical replication launcher

pgBackRest is set up and initialized for started instances:

::

    user@managed:~$ tree -L 2  $tmpdir/backups/backup
    /tmp/.../backups/backup
    ├── 13-preprod
    │   ├── backup.info
    │   └── backup.info.copy
    └── 13-prod
        ├── backup.info
        └── backup.info.copy

And a systemd timer has been added for our instances:
::

    user@managed:~$ systemctl --user list-timers
    NEXT                          LEFT    LAST PASSED UNIT                               ACTIVATES
    Sat 2021-04-03 00:00:00 CEST  7h left n/a  n/a    postgresql-backup@13-preprod.timer postgresql-backup@13-preprod.service
    Sat 2021-04-03 00:00:00 CEST  7h left n/a  n/a    postgresql-backup@13-prod.timer    postgresql-backup@13-prod.service

    2 timers listed.

Instances update
----------------

In the following version of our previous playbook, we are dropping the "preprod"
instance and set the "dev" one to be ``started`` while changing a bit its
configuration:

.. literalinclude:: ../ansible/play2.yml
    :language: yaml
    :caption: play2.yml

As you can see you can feed third-party ansible modules (like
``community.postgresql``) with libpq environment variables obtained by
``dalibo.pglift.instance`` or ``dalibo.pglift.dsn_info``.

::

    user@control:~$ ansible-playbook --extra-vars pglift-vars --ask-vault-password play2.yml
    PLAY [my postgresql instances] ***************************************************************************

    TASK [Gathering Facts] ***********************************************************************************
    ok: [localhost]

    TASK [production instance] *******************************************************************************
    ok: [localhost]

    TASK [pre-production instance, now dropped] **************************************************************
    ok: [localhost]

    TASK [dev instance, started, with SSL] *******************************************************************
    changed: [localhost]

    PLAY RECAP ***********************************************************************************************
    localhost                  : ok=4    changed=1    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0

::

    user@managed:~$ tree -L 2 $tmpdir/postgres
    /tmp/.../postgres
    └── 13
        ├── dev
        └── prod


Cleanup
-------

Finally, in this last playbook, we drop all our instances:

.. literalinclude:: ../ansible/play3.yml
    :language: yaml
    :caption: play3.yml

::

    user@control:~$ ansible-playbook --extra-vars @$tmpdir/vars --ask-vault-password play3.yml
    PLAY [my postgresql instances] ***************************************************************************

    TASK [Gathering Facts] ***********************************************************************************
    ok: [localhost]

    TASK [production instance, dropped] **********************************************************************
    ok: [localhost]

    TASK [dev instance, dropped] *****************************************************************************
    ok: [localhost]

    PLAY RECAP ***********************************************************************************************
    localhost                  : ok=3    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0

::

    user@managed:~$ tree -L 2 $tmpdir/postgres
    /tmp/.../postgres
    └── 13

.. _`community.postgresql`: https://galaxy.ansible.com/community/postgresql
