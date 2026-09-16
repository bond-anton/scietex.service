scietex.service.task_handler
============================

Task handler subsystem: the ``TaskHandler`` base class, typed task schemas,
the built-in ``cancel_task`` handler and the versioned transport wire helpers.

.. automodule:: scietex.service.task_handler
   :no-members:

.. autoclass:: scietex.service.task_handler.TaskHandler
   :members:
.. autoclass:: scietex.service.task_handler.TaskHandlerContext
   :members:
.. autoclass:: scietex.service.task_handler.CancelTaskHandler
   :members:
.. autoclass:: scietex.service.task_handler.CancelTaskRequest
.. autoclass:: scietex.service.task_handler.CancelTaskResponse
.. autoclass:: scietex.service.task_handler.TaskData
.. autoclass:: scietex.service.task_handler.TaskResult
.. autoclass:: scietex.service.task_handler.TaskTimeout
.. autoclass:: scietex.service.task_handler.TaskStatus
.. autoclass:: scietex.service.task_handler.TaskTracker
.. autoclass:: scietex.service.task_handler.TaskEnvelope
.. autofunction:: scietex.service.task_handler.encode_task_envelope
.. autofunction:: scietex.service.task_handler.decode_task_envelope
