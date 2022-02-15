Using Ansible modules
=====================

.. highlight:: console

This tutorial illustrates the use of the Ansible modules shipped with pglift:
``dalibo.pglift.instance``, ``dalibo.pglift.database`` and
``dalibo.pglift.role``.

.. note:: Documentation for each module can be obtained by using ``ansible-doc
   <modulename>`` (possibly after setting ``ANSIBLE_COLLECTIONS_PATHS`` as
   described below); e.g.:

   ::

       $ ansible-doc dalibo.pglift.role
       > DALIBO.PGLIFT.ROLE
       (.../ansible/ansible_collections/dalibo/pglift/plugins/modules/role.py)

       Manage roles of a PostgreSQL server instance

       OPTIONS (= is mandatory):

       [...]


First, ``ansible`` needs to be installed in the :ref:`development environment
<devenv>`:

::

    (.venv) $ pip install ansible

The following playbook installs and configures 3 PostgreSQL instances on
localhost; the first two ones are *started* while the third one is not.

.. literalinclude:: ../ansible/play1.yml
    :language: yaml
    :caption: docs/ansible/play1.yml

To exercice this playbook on a regular user, the system configuration first
needs to be adjusted in order to define a writable directory to host
PostgreSQL instances, data and configuration files:

::

    $ tmpdir=$(mktemp -d)

::

    $ cat > ~/.config/pglift/settings.yaml << EOF
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
    prometheus: {}
    EOF

::

    $ export ANSIBLE_COLLECTIONS_PATHS="./ansible/"

.. note::
   If using `systemd` as service manager and/or scheduler as in above example,
   an extra installation step is needed as documented :ref:`here
   <systemd_install>`.

The passwords for `postgres` and `bob` users can be encrypted using ansible
vault, `ansible-vault encrypt` will ask for a passphrase used to encrypt
variables:

::

    $ cat << EOF | ansible-vault encrypt > $tmpdir/vars
    postgresql_surole_password: $(openssl rand -base64 9)
    prod_bob_password: $(openssl rand -base64 9)
    EOF

To view actual passwords:

::

    ansible-vault view $tmpdir/vars

Finally, run:

::

    (.venv) $ ansible-playbook --extra-vars @$tmpdir/vars docs/ansible/play1.yml
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

We can see our instances installed and running:

::

    $ tree -L 3 $tmpdir/postgres
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
    $ ps xf
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

    $ tree -L 2  $tmpdir/backups/backup
    /tmp/.../backups/backup
    ├── 13-preprod
    │   ├── backup.info
    │   └── backup.info.copy
    └── 13-prod
        ├── backup.info
        └── backup.info.copy

And a systemd timer has been added for our instances:
::

    $ systemctl --user list-timers
    NEXT                          LEFT    LAST PASSED UNIT                               ACTIVATES
    Sat 2021-04-03 00:00:00 CEST  7h left n/a  n/a    postgresql-backup@13-preprod.timer postgresql-backup@13-preprod.service
    Sat 2021-04-03 00:00:00 CEST  7h left n/a  n/a    postgresql-backup@13-prod.timer    postgresql-backup@13-prod.service

    2 timers listed.

In the following version of our previous playbook, we are dropping the "preprod"
instance and set the "dev" one to be ``started`` while changing a bit its
configuration:

.. literalinclude:: ../ansible/play2.yml
    :language: yaml
    :caption: docs/ansible/play2.yml

::

    (.venv) $ ansible-playbook --extra-vars @$tmpdir/vars docs/ansible/play2.yml
    PLAY [my postgresql instances] ***************************************************************************

    TASK [Gathering Facts] ***********************************************************************************
    ok: [localhost]

    TASK [production instance] *******************************************************************************
    ok: [localhost]

    TASK [pre-production instance, now dropped] **************************************************************
    ok: [localhost]

    TASK [dev instance, started, with SSL] *******************************************************************
    --- before: 13/dev
    +++ after: 13/dev
    @@ -1,5 +1,5 @@
     {
    -    "max_connections": null,
    -    "port": 5444,
    -    "ssl": null
    +    "max_connections": 42,
    +    "port": 5455,
    +    "ssl": true
     }

    changed: [localhost]

    PLAY RECAP ***********************************************************************************************
    localhost                  : ok=4    changed=1    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0

::

    $ tree -L 2 $tmpdir/postgres
    /tmp/.../postgres
    └── 13
        ├── dev
        └── prod


Finally, in this last playbook, we drop all our instances:

.. literalinclude:: ../ansible/play3.yml
    :language: yaml
    :caption: docs/ansible/play3.yml

::

    (.venv) $ ansible-playbook docs/ansible/play3.yml
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

    $ tree -L 2 $tmpdir/postgres
    /tmp/.../postgres
    └── 13
