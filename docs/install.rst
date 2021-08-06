Installation
============

pglift can be installed from PyPI, for instance in a virtualenv as follow:

::

    $ python3 -m venv .venv
    $ . .venv/bin/activate
    (.venv) $ pip install pglift

The :doc:`Ansible <ansible>` collection is not shipped with the Python package, so
follow the :doc:`development setup <dev>` to use the Ansible interface.

After package installation, it is necessary to also install data files using:

::

    (.venv) $ pglift site-configure install

This will essentially install systemd templates into
``~/.local/share/systemd/user/``. Using the ``uninstall`` argument of
``site-configure`` command would uninstall those.

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
