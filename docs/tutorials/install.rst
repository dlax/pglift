.. _install:

Installation
============

pglift can be installed from PyPI.

First, it is recommended to use a dedicated virtualenv:
::

    $ python3 -m venv .venv
    $ . .venv/bin/activate
    (.venv) $ pip install 'pip>=20.3'

then proceed with actual installation as:
::

    (.venv) $ pip install pglift "psycopg[binary]"

.. warning::
   Some old platforms, e.g. CentOS 7, do not support *binary* installation of
   psycopg so one should follow the `local installation
   <https://www.psycopg.org/psycopg3/docs/basic/install.html#local-installation>`_
   instructions.
.. https://github.com/psycopg/psycopg/issues/180

The :doc:`Ansible <ansible>` collection is not shipped with the
Python package, so follow the :doc:`development setup <../dev>` to use the
Ansible interface.

.. note::
   If usage of systemd as service manager and/or scheduler is planned,
   additional steps might be needed, see :ref:`detailed systemd installation
   instructions <systemd_install>`.

Once installed, the ``pglift`` command should be available:

::

    $ pglift
    Usage: pglift [OPTIONS] COMMAND [ARGS]...

      Deploy production-ready instances of PostgreSQL

    Options:
      --log-level [DEBUG|INFO|WARNING|ERROR|CRITICAL]
      --help                          Show this message and exit.

    Commands:
      database  Manipulate databases
      instance  Manipulate instances
      role      Manipulate roles

Runtime dependencies
--------------------

pglift operates PostgreSQL and a number of satellite components, each
available as independent software packages. Thus, depending of selected
components (see :ref:`site settings <settings>`), the following packages might
be needed:

- ``postgresql``
- ``pgbackrest``
- ``prometheus-postgres-exporter``
- ``powa`` (with ``pg_qualstats`` and ``pg_stat_kcache``)

Shell completion
----------------

pglift comes with completion scripts for your favorite shell. You can activate
completion for ``bash``, ``zsh`` or ``fish``.

Bash
~~~~

::

  $ source <(pglift completion bash)

  # To load completions for each session, execute once:
  $ pglift completion bash > /etc/bash_completion.d/pglift

Zsh
~~~

::

  $ pglift completion zsh > "${fpath[1]}/pglift"

Fish
~~~~

::

  $ pglift completion fish | source

  # To load completions for each session, execute once:
  $ pglift completion fish > ~/.config/fish/completions/pglift.fish
