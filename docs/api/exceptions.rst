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
