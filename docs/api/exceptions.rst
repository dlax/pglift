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

  * :class:`InvalidVersion`

  * :class:`InstanceStateError`

  * :class:`CommandError`

Exception classes
-----------------

.. autoexception:: Error
.. autoexception:: NotFound
   :members: object_type
.. autoexception:: InstanceNotFound
.. autoexception:: RoleNotFound
.. autoexception:: DatabaseNotFound
.. autoexception:: CommandError
.. autoexception:: InvalidVersion
.. autoexception:: InstanceStateError
