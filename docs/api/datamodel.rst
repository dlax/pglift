Data model
==========

Data may come from two distinct sources in pglift: either as objects built by
inspecting the system or as *manifest* documents obtained from the user
through an interface. The former kind of model is referred to as the *system
model* whereas the later is called the *interface model*.

System model
------------

.. currentmodule:: pglift.models.system

.. autoclass:: PostgreSQLInstance
   :members:
.. autoclass:: Instance
   :members:

Interface model
---------------

.. currentmodule:: pglift.models.interface

.. autoclass:: Manifest
    :members:
.. autoclass:: Instance
    :members:
.. autoclass:: InstanceState
    :members:
.. autoclass:: InstanceListItem
    :members:
.. autoclass:: Role
    :members:
.. autoclass:: Database
    :members:
.. autoclass:: DetailedDatabase
    :members:
.. autoclass:: Tablespace
    :members:
.. autoclass:: Privilege
    :members:
.. autoclass:: PGSetting
    :members:
