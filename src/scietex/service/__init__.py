"""scietex.service — Async worker framework for building background daemon services.

Core classes:
    - ``BasicWorker``: Base async worker with signal handling, logging,
      heartbeat and watchdog managers, and graceful shutdown support.
    - ``TaskProcessor``: Extends ``BasicWorker`` with a task queue,
      concurrent task processing, handler dispatch, and timeout monitoring.
    - ``ValkeyWorker``: Extends ``TaskProcessor`` with Valkey (Redis)
      integration via the ``glide`` client for distributed task queues.
      (Requires ``scietex.service[valkey]`` extra.)
    - ``MqttWorker``: Extends ``TaskProcessor`` with MQTT 5 integration via
      the ``aiomqtt`` client for distributed task queues.
      (Requires ``scietex.service[mqtt]`` extra.)

Module-level exports:
    ``__version__``, ``BasicWorker``, ``TaskProcessor``, and
    optionally ``ValkeyWorker``/``MqttWorker`` and their configuration
    classes.

The ``VALKEY_AVAILABLE`` and ``MQTT_AVAILABLE`` flags report whether the
respective surfaces could be imported at package load time.
"""

import logging

from .basic_worker import BasicWorker
from .config import TaskProcessorConfig, WorkerConfig
from .manager import Manager, register_manager
from .task_processor import TaskProcessor
from .transport import InMemoryTransport, TaskSink, TaskTransport
from .transport_worker import TransportWorker
from .version import __version__

__all__ = [
    "__version__",
    "TaskProcessor",
    "BasicWorker",
    "Manager",
    "register_manager",
    "TaskProcessorConfig",
    "WorkerConfig",
    "TaskTransport",
    "TaskSink",
    "InMemoryTransport",
    "TransportWorker",
]

VALKEY_AVAILABLE = False
try:
    from .valkey import (
        ValkeyAdvancedConfig,
        ValkeyBackoffStrategy,
        ValkeyBaseConfig,
        ValkeyConfig,
        ValkeyNode,
        ValkeyPubSubConfig,
        ValkeyTlsAdvancedConfiguration,
        ValkeyUserCredentials,
        ValkeyWorker,
        ValkeyWorkerConfig,
    )

    VALKEY_AVAILABLE = True

    __all__ += [
        "ValkeyWorker",
        "ValkeyNode",
        "ValkeyUserCredentials",
        "ValkeyBackoffStrategy",
        "ValkeyBaseConfig",
        "ValkeyConfig",
        "ValkeyAdvancedConfig",
        "ValkeyPubSubConfig",
        "ValkeyTlsAdvancedConfiguration",
        "ValkeyWorkerConfig",
    ]
except ImportError:
    # If the Valkey dependency (glide) is missing, swallow the ImportError so
    # the package remains importable without the valkey extra installed.
    # Real bugs in the valkey module or a broken glide install must not be
    # hidden: they raise non-ImportError exceptions that propagate.
    logging.getLogger(__name__).warning(
        "Valkey support unavailable: install the 'valkey' extra (scietex.service[valkey]) to enable ValkeyWorker."
    )

MQTT_AVAILABLE = False
try:
    from .mqtt import (
        MqttConfig,
        MqttTransport,
        MqttWorker,
        MqttWorkerConfig,
        read_mqtt_config,
    )

    MQTT_AVAILABLE = True

    __all__ += [
        "MqttWorker",
        "MqttTransport",
        "MqttConfig",
        "MqttWorkerConfig",
        "read_mqtt_config",
    ]
except ImportError:
    # If the MQTT dependency (aiomqtt) is missing, swallow the ImportError so
    # the package remains importable without the mqtt extra installed.
    # Real bugs in the mqtt module or a broken aiomqtt install must not be
    # hidden: they raise non-ImportError exceptions that propagate.
    logging.getLogger(__name__).warning(
        "MQTT support unavailable: install the 'mqtt' extra (scietex.service[mqtt]) to enable MqttWorker."
    )
