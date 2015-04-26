=========
Reference
=========

.. automodule:: LowVoltage

Connection
==========

.. automodule:: LowVoltage.connection.connection
    :no-members:

.. autoclass:: LowVoltage.connection.connection.Connection

    .. automethod:: __call__

Credentials
-----------

.. automodule:: LowVoltage.connection.credentials

Retry policies
--------------

.. automodule:: LowVoltage.connection.retry_policies

Attribute types
===============

.. automodule:: LowVoltage.attribute_types

.. _python-types:

Python types
============

.. automodule:: LowVoltage.actions.conversion

Exceptions
==========

.. toctree::

    reference/exceptions

.. _actions:

Actions
=======

See also :ref:`actions-vs-compounds` in the user guide.

Admin actions
-------------

.. toctree::

    reference/actions/create_table
    reference/actions/describe_table
    reference/actions/update_table
    reference/actions/delete_table
    reference/actions/list_tables

Item actions
------------

.. toctree::

    reference/actions/put_item
    reference/actions/get_item
    reference/actions/update_item
    reference/actions/delete_item

Batch actions
-------------

.. toctree::

    reference/actions/batch_get_item
    reference/actions/batch_write_item
    reference/actions/query
    reference/actions/scan

Return types
------------

.. toctree::

    reference/actions/return_types

.. _compounds:

Compounds
=========

See also :ref:`actions-vs-compounds` in the user guide.

.. toctree::

    reference/compounds/batch_get_item_iterator
    reference/compounds/batch_put_item
    reference/compounds/batch_delete_item
    reference/compounds/list_tables_iterator
    reference/compounds/scan_iterator
    reference/compounds/query_iterator
    reference/compounds/wait_for_table_activation
    reference/compounds/wait_for_table_deletion
