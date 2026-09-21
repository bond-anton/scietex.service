scietex.service.task_handler
============================

Task handler subsystem: the ``TaskHandler`` base class, typed task schemas,
the built-in ``task:cancel`` and ``worker:*`` handlers, and the versioned
transport wire helpers.

.. automodule:: scietex.service.task_handler
   :no-members:

.. autoclass:: scietex.service.task_handler.TaskHandler
   :members:
.. autoclass:: scietex.service.task_handler.TaskHandlerContext
   :members:
.. autoclass:: scietex.service.task_handler.TaskCapabilities
   :members:
.. autoclass:: scietex.service.task_handler.CancelTaskHandler
   :members:
.. autoclass:: scietex.service.task_handler.CancelTaskRequest
.. autoclass:: scietex.service.task_handler.CancelTaskResponse
.. autodata:: scietex.service.task_handler.CancelOutcome
.. autodata:: scietex.service.task_handler.CancelReason
.. autodata:: scietex.service.task_handler.CancelCallback
.. autodata:: scietex.service.task_handler.TASK_CANCEL_TASK_NAME
.. autoclass:: scietex.service.task_handler.WorkerControlHandler
   :members:
.. autoclass:: scietex.service.task_handler.WorkerControlRequest
.. autoclass:: scietex.service.task_handler.WorkerControlResponse
.. autodata:: scietex.service.task_handler.WorkerAction
.. autodata:: scietex.service.task_handler.WorkerActionCallback
.. autodata:: scietex.service.task_handler.WORKER_START_TASK_NAME
.. autodata:: scietex.service.task_handler.WORKER_STOP_TASK_NAME
.. autodata:: scietex.service.task_handler.WORKER_RESTART_TASK_NAME
.. autodata:: scietex.service.task_handler.WORKER_EXIT_TASK_NAME
.. autodata:: scietex.service.task_handler.CONTROL_TASK_NAMES
.. autoclass:: scietex.service.task_handler.TaskData
.. autoclass:: scietex.service.task_handler.TaskResult
.. autoclass:: scietex.service.task_handler.TaskTimeout
.. autoclass:: scietex.service.task_handler.TaskStatus
.. autoclass:: scietex.service.task_handler.TaskProgress
.. autoclass:: scietex.service.task_handler.TaskTracker
.. autoclass:: scietex.service.task_handler.TaskEnvelope
.. autofunction:: scietex.service.task_handler.encode_task_envelope
.. autofunction:: scietex.service.task_handler.decode_task_envelope
.. autofunction:: scietex.service.task_handler.decode_task_envelope_version
