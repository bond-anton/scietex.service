scietex.service.client
======================

Client-side worker registry and watcher: a read-only view over the worker
heartbeats published by the transports. The core is dependency-free; the
concrete backends live in the transport packages
(:class:`~scietex.service.valkey.watch.PollingBackend`,
:class:`~scietex.service.mqtt.watch.SubscribeBackend`).

.. automodule:: scietex.service.client
   :no-members:

.. autoclass:: scietex.service.client.WorkerWatcher
   :members:
.. autoclass:: scietex.service.client.WorkerRegistry
   :members:
.. autoclass:: scietex.service.client.WorkerRecord
   :members:
.. autoclass:: scietex.service.client.WorkerEvent
   :members:
.. autoclass:: scietex.service.client.WorkerEventKind
   :members:
.. autoclass:: scietex.service.client.WatchBackend
   :members:
