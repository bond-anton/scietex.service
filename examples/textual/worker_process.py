"""Run a :class:`~scietex.service.BasicWorker` in its own process.

The TUI runs every worker as an asyncio task on the app's single event loop,
which is CPU-bound by valkey-glide's FFI (~7,000 tasks/s per process, divided
among N workers). Moving each worker into its own OS subprocess gives it an
event loop and a CPU core of its own, so one busy worker no longer steals
throughput from the others.

The child owns the worker (Option B, mirroring :mod:`producer_process`): the
parent never constructs a worker or opens a broker connection, so ``start`` and
``stop`` are commands to the child rather than local calls.

IPC is three ``multiprocessing`` queues:

* ``commands`` (parent -> child): a :class:`WorkerCommand` per request.
* ``snapshots`` (child -> parent): the latest :class:`WorkerSnapshot`, pushed
  after every command and on a heartbeat so the parent's poll tick stays fresh.
* ``logs`` (child -> parent): :class:`LogRecordData` records, drained by the
  parent into the slot's log panel.

Worker commands are fire-and-forget: ``start``/``stop`` take seconds and the app
only polls ``snapshot()`` on its 0.5s tick, so there is no reply handshake and
no ``seq`` tagging. The child is spawned, not forked: the parent is a running
asyncio app, and a fork would duplicate its loop and its broker connections.
"""

import asyncio
import logging
import multiprocessing as mp
import queue
import time
from dataclasses import dataclass
from multiprocessing.context import SpawnProcess
from multiprocessing.queues import Queue
from typing import Any

from scietex.logging import ConsoleHandler

from scietex.service import ScietexDark, TaskProcessor
from scietex.service.basic_worker import ServiceStatus
from scietex.service.task_metrics import TaskMetricsSnapshot

from .ui_worker import UiMqttWorker, UiValkeyWorker, build_ui_worker

#: How often the child pushes an untagged snapshot while it runs, so the
#: parent's metrics advance between commands.
SNAPSHOT_INTERVAL = 0.25

#: How long the parent waits for the child to exit on shutdown before killing it.
SHUTDOWN_TIMEOUT = 5.0

#: How long the child waits for the worker to reach STOPPED after an ``exit``
#: command before returning anyway. ``exit()`` only spawns the shutdown task, so
#: the child polls the state machine for the terminal transition.
CHILD_STOP_TIMEOUT = 10.0

#: Bounded size of the log queue, so a wedged UI cannot grow it without bound.
LOG_QUEUE_MAXSIZE = 10000

#: Maps a worker kind string to the UI worker class that serves it. The
#: placeholder classes (when an extra is absent) still resolve here; they raise
#: on construction, which the child reports through its error snapshot.
_WORKER_CLASSES: dict[str, type] = {
    "valkey": UiValkeyWorker,
    "mqtt": UiMqttWorker,
}


@dataclass(frozen=True)
class WorkerIdentity:
    """A worker's cross-boundary identity, replacing the live worker object.

    The card renders this instead of the worker itself: ``kind_label`` is the
    transport label and ``instance_id`` the worker's auto-generated id. A plain
    dataclass so the struct is unambiguously picklable.
    """

    kind_label: str
    instance_id: str


@dataclass(frozen=True)
class LogRecordData:
    """A picklable snapshot of a :class:`logging.LogRecord`.

    ``from_record`` resolves the message child-side, so ``args`` — which may
    hold objects that do not pickle — never crosses the boundary; ``exc_text``
    is captured as a plain string. ``to_log_record`` rebuilds a record the
    parent's ``ScietexFormatter`` can render.
    """

    name: str
    levelno: int
    levelname: str
    message: str
    pathname: str
    lineno: int
    funcName: str
    created: float
    msecs: float
    exc_text: str | None

    @classmethod
    def from_record(cls, record: logging.LogRecord) -> "LogRecordData":
        """Build from a live record, resolving ``args`` into ``message`` child-side."""
        return cls(
            name=record.name,
            levelno=record.levelno,
            levelname=record.levelname,
            message=record.getMessage(),
            pathname=record.pathname,
            lineno=record.lineno,
            funcName=record.funcName,
            created=record.created,
            msecs=record.msecs,
            exc_text=record.exc_text,
        )

    def to_log_record(self) -> logging.LogRecord:
        """Rebuild a :class:`logging.LogRecord` for the parent's formatter.

        ``makeLogRecord`` leaves ``msg`` empty and ``args`` unset, so a formatter
        that calls ``getMessage()`` would render nothing (or raise on a stale
        ``args``). The resolved text is carried in ``msg`` with ``args`` cleared,
        so ``getMessage()`` returns it verbatim.
        """
        record = logging.makeLogRecord(self.__dict__)
        record.msg = self.message
        record.args = None
        return record


@dataclass(frozen=True)
class WorkerCommand:
    """One parent -> child request.

    ``kind`` selects the operation (``"start"``/``"stop"``/``"exit"``); ``value``
    carries its argument, reserved for future verbs that need one. Worker
    commands are fire-and-forget, so there is no ``seq`` and no reply to await.
    """

    kind: str
    value: Any = None


@dataclass(frozen=True)
class WorkerSnapshot:
    """One child -> parent snapshot of the worker's live state.

    Every field has a default so a placeholder instance can be built before the
    first child snapshot arrives.
    """

    state: str = "Stopped"
    instance_id: str = ""
    running_tasks: int = 0
    max_concurrent: int = 0
    queue_depth: int = 0
    queue_capacity: int = 0
    rate: float = 0.0
    total: int = 0
    failed_managers: tuple[str, ...] = ()
    health_connected: bool | None = None
    health_degraded: bool = False
    error: str | None = None
    exited: bool = False


@dataclass(frozen=True)
class WorkerHealth:
    """Collapsed transport-health view for the card's status line."""

    connected: bool
    degraded: bool


class _ProcessLogHandler(logging.Handler):
    """Forward the child worker's records to the parent's log queue.

    The child removes the worker's ``ConsoleHandler`` so it does not write to
    the child's stdout; this handler replaces it, pickling each record across
    the process boundary for the parent to render.
    """

    def __init__(self, logs: "Queue[LogRecordData]") -> None:
        super().__init__()
        self._logs = logs

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._logs.put_nowait(LogRecordData.from_record(record))
        except queue.Full:
            # A wedged UI must not back-pressure the worker's event loop: the
            # log queue is best-effort, so when it fills the record is dropped
            # rather than letting a slow parent stall task processing.
            pass


def _child_main(
    kind: str,
    log_level: int,
    memory: bool,
    broker_logging: bool,
    commands: "Queue[WorkerCommand]",
    snapshots: "Queue[WorkerSnapshot]",
    logs: "Queue[LogRecordData]",
) -> None:
    """Child entrypoint: own a worker and serve commands until told to exit.

    Runs its own event loop, so the worker's task processing is scheduled
    independently of the parent's loop.
    """
    asyncio.run(_child_loop(kind, log_level, memory, broker_logging, commands, snapshots, logs))


async def _child_loop(
    kind: str,
    log_level: int,
    memory: bool,
    broker_logging: bool,
    commands: "Queue[WorkerCommand]",
    snapshots: "Queue[WorkerSnapshot]",
    logs: "Queue[LogRecordData]",
) -> None:
    try:
        worker = build_ui_worker(
            kind, theme=ScietexDark(show_banner=False), memory=memory, broker_logging=broker_logging
        )
    except Exception as exc:
        # A worker that cannot be built (e.g. its extra is absent) is reported
        # once and the child exits; the parent's startup_error surfaces it.
        snapshots.put(WorkerSnapshot(error=str(exc), exited=True))
        return
    for handler in list(worker.logger.handlers):
        if isinstance(handler, ConsoleHandler):
            worker.logger.removeHandler(handler)
    # The worker defaults to DEBUG, which logs each task's full TaskData struct
    # twice. At the demo's task rates that is thousands of records per second,
    # and the parent renders every one on its event loop, so the UI stalls. INFO
    # keeps the per-task summary and drops the struct dumps; ``--debug`` opts
    # back into the full DEBUG volume.
    worker.logger.setLevel(log_level)
    worker.logger.addHandler(_ProcessLogHandler(logs))
    snapshots.put(_snapshot(worker))
    last_push = time.monotonic()
    try:
        while True:
            # Drain commands without blocking so the snapshot heartbeat below
            # still fires while the worker runs.
            try:
                command = commands.get_nowait()
            except queue.Empty:
                command = None
            if command is not None:
                if command.kind == "exit":
                    await worker.exit()
                    # exit() only spawns the shutdown task, so poll the state
                    # machine for the terminal transition before giving up.
                    deadline = time.monotonic() + CHILD_STOP_TIMEOUT
                    while worker.state is not ServiceStatus.STOPPED and time.monotonic() < deadline:
                        await asyncio.sleep(0.01)
                    snapshots.put(_snapshot(worker, exited=True))
                    return
                if command.kind == "start":
                    await worker.start()
                elif command.kind == "stop":
                    await worker.stop()
                snapshots.put(_snapshot(worker))
                last_push = time.monotonic()
            now = time.monotonic()
            if now - last_push >= SNAPSHOT_INTERVAL:
                snapshots.put(_snapshot(worker))
                last_push = now
            await asyncio.sleep(0.01)
    finally:
        await worker.stop()


def _snapshot(worker: TaskProcessor, *, exited: bool = False) -> WorkerSnapshot:
    """Build a :class:`WorkerSnapshot` from the child's live worker."""
    metrics = worker.task_metrics()
    health = getattr(worker, "transport_health", None)
    return WorkerSnapshot(
        state=worker.state.value,
        instance_id=worker.instance_id,
        running_tasks=metrics.running,
        max_concurrent=worker.max_concurrent_tasks,
        queue_depth=metrics.queue_depth,
        queue_capacity=worker.queue_size,
        rate=metrics.rate,
        total=metrics.total,
        failed_managers=tuple(worker.failed_managers),
        health_connected=None if health is None else health.connected,
        health_degraded=False if health is None else health.degraded,
        exited=exited,
    )


class WorkerProcess:
    """Parent-side handle to a worker running in a child process.

    Mirrors the :class:`~scietex.service.task_processor.TaskProcessor` surface the
    app reads, but every mutating call is a command to the child and every read
    comes from the last snapshot the child pushed. Reads are synchronous: the
    app's poll tick is synchronous, and a command's effect shows up in a later
    snapshot.
    """

    def __init__(
        self, kind: str, *, log_level: int = logging.INFO, memory: bool = False, broker_logging: bool = False
    ) -> None:
        self._kind = kind
        self._log_level = log_level
        self._memory = memory
        self._broker_logging = broker_logging
        self._ctx = mp.get_context("spawn")
        self._commands: Queue[WorkerCommand] = self._ctx.Queue()
        self._snapshots: Queue[WorkerSnapshot] = self._ctx.Queue()
        self._logs: Queue[LogRecordData] = self._ctx.Queue(maxsize=LOG_QUEUE_MAXSIZE)
        self._process: SpawnProcess | None = None
        self._snapshot = WorkerSnapshot()
        self._identity: WorkerIdentity | None = None
        self._log_buffer: list[LogRecordData] = []

    @property
    def state(self) -> ServiceStatus:
        return ServiceStatus(self._snapshot.state)

    @property
    def instance_id(self) -> str:
        return self._snapshot.instance_id

    def task_metrics(self) -> TaskMetricsSnapshot:
        """The worker's processing metrics, rebuilt from the latest snapshot."""
        return TaskMetricsSnapshot(
            queue_depth=self._snapshot.queue_depth,
            running=self._snapshot.running_tasks,
            rate=self._snapshot.rate,
            total=self._snapshot.total,
        )

    @property
    def max_concurrent_tasks(self) -> int:
        return self._snapshot.max_concurrent

    @property
    def queue_size(self) -> int:
        return self._snapshot.queue_capacity

    @property
    def failed_managers(self) -> tuple[str, ...]:
        return self._snapshot.failed_managers

    @property
    def transport_health(self) -> WorkerHealth | None:
        """Collapsed transport-health view, or ``None`` for a transport-less worker."""
        if self._snapshot.health_connected is None:
            return None
        return WorkerHealth(
            connected=self._snapshot.health_connected,
            degraded=self._snapshot.health_degraded,
        )

    @property
    def kind_label(self) -> str:
        """Transport label for the worker kind, read from the class's ``_transport_name``."""
        cls = _WORKER_CLASSES.get(self._kind)
        if cls is None:
            return self._kind
        name = getattr(cls, "_transport_name", None)
        return name if name is not None else cls.__name__

    @property
    def identity(self) -> WorkerIdentity:
        """The worker's identity, placeholder until the first snapshot arrives."""
        if self._identity is None:
            return WorkerIdentity(self.kind_label, "····")
        return self._identity

    @property
    def startup_error(self) -> str | None:
        return self._snapshot.error

    @property
    def exited(self) -> bool:
        """Whether the worker exited, or its child process died unexpectedly."""
        return self._snapshot.exited or (self._process is not None and not self._process.is_alive())

    def start_process(self) -> None:
        """Spawn the child. Idempotent while the child is alive."""
        if self._process is not None and self._process.is_alive():
            return
        self._process = self._ctx.Process(
            target=_child_main,
            args=(
                self._kind,
                self._log_level,
                self._memory,
                self._broker_logging,
                self._commands,
                self._snapshots,
                self._logs,
            ),
            daemon=True,
        )
        self._process.start()

    async def start(self) -> None:
        """Ask the child to start its worker."""
        await asyncio.to_thread(self._send, WorkerCommand("start"))

    async def stop(self) -> None:
        """Ask the child to stop its worker, keeping the process alive."""
        await asyncio.to_thread(self._send, WorkerCommand("stop"))

    async def exit(self) -> None:
        """Ask the child to exit and join it, killing it if it will not leave."""
        await asyncio.to_thread(self._exit_blocking)

    def snapshot(self) -> WorkerSnapshot:
        """The most recent snapshot the child pushed, refreshed from the queue."""
        self._drain()
        return self._snapshot

    def drain_logs(self) -> list[LogRecordData]:
        """Drain the log queue and return the records buffered since the last call."""
        self._drain()
        logs = self._log_buffer
        self._log_buffer = []
        return logs

    def _send(self, command: WorkerCommand) -> None:
        """Send a command to the child, dropping it if the child is gone.

        Worker commands are fire-and-forget: ``start``/``stop`` take seconds and
        the app only polls ``snapshot()`` on its tick, so there is no reply to
        await. A bare ``put_nowait`` guarded by liveness is enough — the child
        drains its command queue each loop iteration and reflects the effect in
        its next snapshot.
        """
        if self._process is None or not self._process.is_alive():
            return
        self._commands.put_nowait(command)

    def _exit_blocking(self) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        try:
            self._commands.put_nowait(WorkerCommand("exit"))
        except Exception:
            # A full command queue means the child is wedged; the join below
            # will time out and the kill path takes over.
            pass
        process.join(timeout=SHUTDOWN_TIMEOUT)
        if process.is_alive():
            process.terminate()
            process.join(timeout=SHUTDOWN_TIMEOUT)
        if process.is_alive():
            process.kill()
            process.join(timeout=SHUTDOWN_TIMEOUT)

    def _drain(self) -> None:
        """Pull the child's snapshots and logs, keeping the newest snapshot.

        Draining keeps the queues from filling while the child runs: the log
        queue is bounded, so a parent that stops reading would back-pressure the
        child's event loop.
        """
        while True:
            try:
                snapshot = self._snapshots.get_nowait()
            except queue.Empty:
                break
            self._snapshot = snapshot
            if self._identity is None:
                self._identity = WorkerIdentity(self.kind_label, snapshot.instance_id)
        while True:
            try:
                self._log_buffer.append(self._logs.get_nowait())
            except queue.Empty:
                break
