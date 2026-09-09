scietex.service.valkey
======================

Valkey (Redis) integration via the ``glide`` client: ``ValkeyWorker`` and its
configuration structs. Requires the ``valkey`` extra.

.. automodule:: scietex.service.valkey
   :no-members:

.. autoclass:: scietex.service.valkey.ValkeyWorker
   :members:
.. autoclass:: scietex.service.valkey.ValkeyNode
.. autoclass:: scietex.service.valkey.ValkeyUserCredentials
.. autoclass:: scietex.service.valkey.ValkeyBackoffStrategy
.. autoclass:: scietex.service.valkey.ValkeyTlsAdvancedConfiguration
.. autoclass:: scietex.service.valkey.ValkeyAdvancedConfig
.. autoclass:: scietex.service.valkey.ValkeyBaseConfig
.. autoclass:: scietex.service.valkey.ValkeyConfig
.. autoclass:: scietex.service.valkey.ValkeyWorkerConfig
.. autofunction:: scietex.service.valkey.purge_task_stream
