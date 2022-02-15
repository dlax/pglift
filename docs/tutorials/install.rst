Installation
============

pglift can be installed from PyPI, for instance in a virtualenv as follow:

::

    $ python3 -m venv .venv
    $ . .venv/bin/activate
    (.venv) $ pip install pglift

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


Shell completion
----------------

pglift comes with completion scripts for your favorite shell. You can activate
completion for ``bash``, ``zsh`` or ``fish``.

Bash
~~~~

Source the bash complete script ``extras/.pglift-complete.bash`` (for example in ``~/.bashrc`` or ``~/.bash_profile``).

Zsh
~~~

Source the zsh complete script ``extras/.pglift-complete.zsh`` (for example in ``~/.zshrc`` or ``~/.zsh_profile``).

Fish
~~~~

Copy the fish complete script ``extras/.pglift-complete.fish`` to
``~/.config/fish/completions/``.
