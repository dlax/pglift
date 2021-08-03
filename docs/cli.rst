Command Line Interface
======================

Usage
-----

.. highlight:: console

pglift provides a CLI that can be used as follows:

::

    (.venv) $ pglift --help
    Usage: pglift [OPTIONS] COMMAND [ARGS]...

    Deploy production-ready instances of PostgreSQL

    Options:
      ...

    Commands:
      ...

For example, you can describe an instance using the following command:

::

    (.venv) $ pglift instance describe myinstance

The following syntax is also valid:

::

    (.venv) $ python -m pglift instance describe myinstance


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
