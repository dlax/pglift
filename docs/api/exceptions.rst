.. currentmodule:: pglift.exceptions

Exceptions
==========

Exception hierarchy
-------------------

* :class:`Error`

  * :class:`NotFound`

    * :class:`InstanceNotFound`
    * :class:`RoleNotFound`
    * :class:`DatabaseNotFound`

Exception classes
-----------------

.. autoclass:: Error
.. autoclass:: NotFound
   :members: object_type
.. autoclass:: InstanceNotFound
.. autoclass:: RoleNotFound
.. autoclass:: DatabaseNotFound
