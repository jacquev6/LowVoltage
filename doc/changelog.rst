=========
Changelog
=========

????/??/?? v0.7.0
==================

- A few hours after writing "I believe the interface is stable", I had an aha moment and had to change the :ref:`compounds`, making them all functions instead of a mix of classes and badly named functions.
- The constructors of :ref:`Actions` now optionaly accept the mandatory parameters of the action, as descibed in :ref:`building-actions`. I had to change the :class:`.BatchGetItem` and :class:`.BatchWriteItem` constructor: they don't accept unprocessed keys/items anymore.

2015/04/26, v0.6.0
==================

- This is the first beta version, so the first to appear in the changelog.
