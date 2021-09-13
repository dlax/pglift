Installation
============

pglift can be installed from PyPI, for instance in a virtualenv as follow:

::

    $ python3 -m venv .venv
    $ . .venv/bin/activate
    (.venv) $ pip install "pglift[cli]"

The :doc:`Ansible <tutorials/ansible>` collection is not shipped with the
Python package, so follow the :doc:`development setup <dev>` to use the
Ansible interface.

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
