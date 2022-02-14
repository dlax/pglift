.. _settings:

Settings
========

The details of most operations can be configured by defining the *settings* of
an installation. These settings are usually gathered in a configuration file,
in YAML format, and some can be overridden from environment variables.

A typical settings document looks like:

.. literalinclude:: ../../files/example-settings.yaml
   :language: yaml

Apart from ``postgresql``, most top-level keys correspond to *satellite
components* of instances and their value thus defines how these components are
installed, configured, run. Some other top-level keys correspond to
cross-service settings defining, e.g., how scheduled tasks are run or which
service manager is used.

Many fields accept a ``null`` value to disable their respective service (e.g.
``scheduler: null``). (In fact, many fields have a default ``null`` value.)
The settings document is merged recursively with default value. In particular,
to enable and use the default configuration for a component, set its value to
an empty object as done for ``pgbackrest`` in the example above.

To view current settings, run:

::

    $ pglift site-settings


Site (or installation) settings are looked up for at the following locations:

- ``$XDG_CONFIG_DIR/pglift/settings.yaml`` [#xdgconfighome]_, then,
- ``/etc/pglift/settings.yaml``.

Once one of these files is found, processing stops.

.. note::

    To temporarily override installed settings, the ``SETTINGS`` environment
    variable can be used. It accepts either a JSON-dumped value or a file path,
    prepended with ``@``:

    ::

        $ SETTINGS='{"postgresql": {"root": "/srv/postgres"}}'
        $ SETTINGS=@/path/to/config.json

.. [#xdgconfighome]
   Where ``$XDG_CONFIG_DIR`` would be ``$HOME/.config`` unless configured
   differently.
