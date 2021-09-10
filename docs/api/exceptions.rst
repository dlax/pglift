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

  * :class:`SystemError`

    * :class:`FileExistsError`
    * :class:`FileNotFoundError`

  * :class:`ConfigurationError`

Exception classes
-----------------

.. autoexception:: Error
.. autoexception:: NotFound
   :members: object_type
.. autoexception:: InstanceNotFound
.. autoexception:: RoleNotFound
.. autoexception:: DatabaseNotFound
.. autoexception:: InvalidVersion
.. autoexception:: InstanceStateError
.. autoexception:: CommandError
.. autoexception:: SystemError
.. autoexception:: FileExistsError
.. autoexception:: FileNotFoundError
.. autoexception:: ConfigurationError
   :members: path
