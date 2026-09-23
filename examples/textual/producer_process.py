"""Run a :class:`~examples.textual.producer.TaskProducer` in its own process.

The producer's interval loop and the worker's task handlers both want the same
event loop, and the handlers win: measured in-process, a producer configured for
10,000 tasks/s only reaches ~1,100/s because its ``asyncio.sleep`` is starved by
the worker's concurrent handlers. Moving the producer into a child process gives
it a loop of its own, so its emit cadence is no longer a function of how busy the
UI's loop is.

The child owns the broker clients (Option B): the parent never opens a Valkey or
MQTT connection for producing, so a one-shot ``Emit`` is a round-trip to the
child rather than a local publish. That keeps a single writer per producer and
avoids two connections racing on the same stream.

IPC is two ``multiprocessing`` queues:

* ``commands`` (parent -> child): a :class:`ProducerCommand` per request.
* ``snapshots`` (child -> parent): the latest :class:`ProducerSnapshot`, pushed
  after every command and on a heartbeat so the parent's counter stays fresh
  while the interval loop runs.

The child is spawned, not forked: the parent is a running asyncio app, and a
fork would duplicate its loop and its broker connections.
"""

import asyncio
import multiprocessing as mp
import time
from dataclasses import dataclass
from multiprocessing.context import SpawnProcess
from multiprocessing.queues import Queue
from typing import Any

from .producer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_INTERVAL_MS,
    DEFAULT_TIMEOUT_MS,
    ProducerSnapshot,
    TaskProducer,
)

#: How long the parent waits for a command's snapshot before giving up. A child
#: that is mid-publish on an unreachable broker can be slow, so this is generous
#: enough to cover one broker timeout but short enough to keep the UI responsive.
COMMAND_TIMEOUT = 5.0

#: How often the child pushes a snapshot while the interval loop runs, so the
#: parent's emitted counter advances between commands.
SNAPSHOT_INTERVAL = 0.25

#: How long the parent waits for the child to exit on shutdown before killing it.
SHUTDOWN_TIMEOUT = 5.0


@dataclass(frozen=True)
class ProducerCommand:
    """One parent -> child request.

    ``kind`` selects the operation; the remaining fields carry its argument.
    A single struct keeps the queue typed and avoids a message class per verb.
    ``seq`` is the parent's monotonically increasing request id: the child echoes
    it on the answering snapshot so the parent can tell a command's reply apart
    from a heartbeat that was already in flight.
    """

    kind: str
    value: Any = None
    seq: int = 0


@dataclass(frozen=True)
class ProducerReply:
    """One child -> parent snapshot, tagged with the command it answers.

    ``seq`` is the ``seq`` of the command that produced this snapshot, or ``0``
    for a heartbeat push that answers no command.
    """

    snapshot: ProducerSnapshot
    seq: int = 0


def _child_main(
    task_name: str,
    service_name: str,
    commands: "Queue[ProducerCommand]",
    replies: "Queue[ProducerReply]",
) -> None:
    """Child entrypoint: own a producer and serve commands until told to stop.

    Runs its own event loop, so the producer's interval loop is scheduled
    independently of the parent's. Every command is answered with a snapshot
    tagged with the command's ``seq`` so the parent can match reply to request.
    """
    asyncio.run(_child_loop(task_name, service_name, commands, replies))


async def _child_loop(
    task_name: str,
    service_name: str,
    commands: "Queue[ProducerCommand]",
    replies: "Queue[ProducerReply]",
) -> None:
    producer = TaskProducer(task_name, service_name=service_name)
    replies.put(ProducerReply(producer.snapshot()))
    last_push = time.monotonic()
    try:
        while True:
            # Drain commands without blocking so the snapshot heartbeat below
            # still fires while the interval loop runs.
            try:
                command = commands.get_nowait()
            except Exception:
                command = None
            if command is not None:
                if command.kind == "stop":
                    await producer.stop()
                    replies.put(ProducerReply(producer.snapshot(), command.seq))
                    return
                await _apply(producer, command)
                replies.put(ProducerReply(producer.snapshot(), command.seq))
                last_push = time.monotonic()
            now = time.monotonic()
            if now - last_push >= SNAPSHOT_INTERVAL:
                replies.put(ProducerReply(producer.snapshot()))
                last_push = now
            await asyncio.sleep(0.01)
    finally:
        await producer.stop()


async def _apply(producer: TaskProducer, command: ProducerCommand) -> None:
    """Apply one command to the child's producer."""
    if command.kind == "emit":
        await producer.emit()
    elif command.kind == "start":
        producer.start()
    elif command.kind == "stop_loop":
        await producer.stop()
    elif command.kind == "valkey":
        producer.set_valkey_enabled(bool(command.value))
    elif command.kind == "mqtt":
        producer.set_mqtt_enabled(bool(command.value))
    elif command.kind == "interval":
        producer.set_interval_ms(int(command.value))
    elif command.kind == "timeout":
        producer.set_timeout_ms(int(command.value))
    elif command.kind == "batch":
        producer.set_batch_size(int(command.value))


class ProducerProcess:
    """Parent-side handle to a producer running in a child process.

    Mirrors the :class:`TaskProducer` surface the app uses, but every mutating
    call is a command to the child and every read comes from the last snapshot
    the child pushed. The handle is deliberately synchronous: the app's poll
    tick is synchronous, and a command's round-trip is bounded by
    :data:`COMMAND_TIMEOUT`.
    """

    def __init__(self, task_name: str, *, service_name: str = "service") -> None:
        self._task_name = task_name
        self._service_name = service_name
        self._ctx = mp.get_context("spawn")
        self._commands: Queue[ProducerCommand] = self._ctx.Queue()
        self._replies: Queue[ProducerReply] = self._ctx.Queue()
        self._process: SpawnProcess | None = None
        self._seq = 0
        self._snapshot = ProducerSnapshot(
            batch_size=DEFAULT_BATCH_SIZE,
            received_at=time.monotonic(),
        )
        self._valkey_enabled = False
        self._mqtt_enabled = False
        self._interval_ms = DEFAULT_INTERVAL_MS
        self._timeout_ms = DEFAULT_TIMEOUT_MS
        self._batch_size = DEFAULT_BATCH_SIZE

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def emitted(self) -> int:
        return self._snapshot.emitted

    @property
    def running(self) -> bool:
        return self._snapshot.running

    @property
    def last_error(self) -> str | None:
        return self._snapshot.last_error

    @property
    def valkey_enabled(self) -> bool:
        return self._valkey_enabled

    @property
    def mqtt_enabled(self) -> bool:
        return self._mqtt_enabled

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def start_process(self) -> None:
        """Spawn the child. Idempotent while the child is alive."""
        if self._process is not None and self._process.is_alive():
            return
        self._process = self._ctx.Process(
            target=_child_main,
            args=(self._task_name, self._service_name, self._commands, self._replies),
            daemon=True,
        )
        self._process.start()

    async def emit(self) -> None:
        """Ask the child to publish one batch."""
        await asyncio.to_thread(self._send, ProducerCommand("emit"))

    def start(self) -> None:
        """Ask the child to start its interval loop."""
        self._send(ProducerCommand("start"))

    async def stop_loop(self) -> None:
        """Ask the child to stop its interval loop, keeping the process alive."""
        await asyncio.to_thread(self._send, ProducerCommand("stop_loop"))

    def set_valkey_enabled(self, enabled: bool) -> None:
        self._valkey_enabled = enabled
        self._send(ProducerCommand("valkey", enabled))

    def set_mqtt_enabled(self, enabled: bool) -> None:
        self._mqtt_enabled = enabled
        self._send(ProducerCommand("mqtt", enabled))

    def set_interval_ms(self, interval_ms: int) -> None:
        self._interval_ms = interval_ms
        self._send(ProducerCommand("interval", interval_ms))

    def set_timeout_ms(self, timeout_ms: int) -> None:
        self._timeout_ms = timeout_ms
        self._send(ProducerCommand("timeout", timeout_ms))

    def set_batch_size(self, batch_size: int) -> None:
        self._batch_size = batch_size
        self._send(ProducerCommand("batch", batch_size))

    def snapshot(self) -> ProducerSnapshot:
        """The most recent snapshot the child pushed, refreshed from the queue."""
        self._drain_replies()
        return self._snapshot

    async def stop(self) -> None:
        """Stop the child and join it, killing it if it will not exit."""
        await asyncio.to_thread(self._stop_blocking)

    def _stop_blocking(self) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        try:
            self._commands.put_nowait(ProducerCommand("stop"))
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

    def _send(self, command: ProducerCommand) -> None:
        """Send a command and wait for the child's answering snapshot.

        The child also pushes untagged heartbeats, so a reply is only accepted
        once its ``seq`` matches this command's. Without that check a heartbeat
        already in flight would be mistaken for the command's effect and the
        caller would read stale state.
        """
        if self._process is None or not self._process.is_alive():
            return
        self._seq += 1
        seq = self._seq
        self._commands.put(ProducerCommand(command.kind, command.value, seq))
        deadline = time.monotonic() + COMMAND_TIMEOUT
        while time.monotonic() < deadline:
            if self._drain_replies(seq):
                return
            time.sleep(0.005)

    def _drain_replies(self, seq: int | None = None) -> bool:
        """Pull queued replies, keeping the newest snapshot.

        Returns True once a reply tagged with ``seq`` has been seen, or, when
        ``seq`` is ``None``, once any reply has been consumed.
        """
        matched = False
        while True:
            try:
                reply = self._replies.get_nowait()
            except Exception:
                return matched
            self._snapshot = reply.snapshot
            if seq is None or reply.seq == seq:
                matched = True
