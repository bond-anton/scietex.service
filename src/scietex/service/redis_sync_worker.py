"""
Provides service class which use Redis server for data storage and pub/sub communication.
"""

from typing import Union
import redis

from .basic_sync_worker import BasicSyncWorker
from .logging.redis_sync import RedisSyncStreamHandler, RedisSyncLogFormatter
from .__helpers import time_ms


class RedisSyncWorker(BasicSyncWorker):
    """Service class with Redis data storage and pub/sub capabilities."""

    def __init__(
        self,
        service_name: str = "service",
        version: str = "0.0.1",
        delay: float = 5,
        **kwargs,
    ) -> None:
        self.__redis_conf = {
            "host": kwargs.get("redis_host", "localhost"),
            "port": kwargs.get("redis_port", 6379),
            "db": kwargs.get("redis_db", 0),
        }
        super().__init__(
            service_name=service_name, version=version, delay=delay, **kwargs
        )
        self.redis_cli: redis.client.Redis = redis.Redis(**self.__redis_conf)
        self.pubsub = self.redis_cli.pubsub()
        self.pubsub.subscribe(
            f"{self.service_name}:{self.worker_id}", f"{self.service_name}:0"
        )
        self.ts = self.redis_cli.ts()
        self.__ts_labels: list[str] = []
        self._get_ts_labels()

        self.redis_cli.hset(
            f"{self.service_name}:{self.worker_id}", "version", value=self.version
        )
        self.redis_cli.hset(
            f"{self.service_name}:{self.worker_id}", "delay", value=str(self.delay)
        )
        self.redis_cli.hset(
            f"{self.service_name}:{self.worker_id}",
            "logging_level",
            value=str(self.logging_level),
        )
        self.redis_cli.hset(
            f"{self.service_name}:{self.worker_id}", "running", value="0"
        )

    def _logger_add_custom_handlers(self) -> None:
        # Create Redis stream handler
        redis_handler = RedisSyncStreamHandler(
            worker_name=self.service_name,
            connection_conf=self.__redis_conf,
            stream_name="worker_logs",
        )
        formatter = RedisSyncLogFormatter()
        redis_handler.setFormatter(formatter)
        self._logger.addHandler(redis_handler)

    @property
    def ts_labels(self) -> list[str]:
        """list of time series labels available"""
        return self.__ts_labels

    def _get_ts_labels(self) -> None:
        """populates list of time series labels, fixes aggregation rules"""
        self.__ts_labels = self.ts.queryindex([f"name={self.service_name}", "type=src"])

    def parse_message(self, msg: dict) -> tuple[str, list[str]]:
        """
        Parses message received on Redis pub/sub channel to extract command and parameters list.
        :param msg: Message dict for parsing.
        :return: command string and a list of parameters.
        """
        cmd: str = ""
        params: list[str] = []
        if msg["type"] == "message":
            self.logger.debug("%d: Got message: %s", self.worker_id, msg)
            params = msg["data"].decode("utf-8").strip().split("::")
            try:
                cmd = params.pop(0)
            except IndexError:
                params = []
            self.logger.debug("%d: CMD: %s", self.worker_id, cmd)
            self.logger.debug("%d: PAR: %s", self.worker_id, params)
        return cmd, params

    def parse_task_data(self, task_data: str) -> tuple[str, list[str]]:
        """
        Parses task data received from Redis to extract command and parameters list.
        :param task_data: task data string for parsing.
        :return: command string and a list of parameters.
        """
        task_name: str = ""
        self.logger.debug("%d: Got task data: %s", self.worker_id, task_data)
        params: list[str] = task_data.strip().split("::")
        try:
            task_name = params.pop(0)
        except IndexError:
            params = []
        self.logger.debug("%d: TASK: %s", self.worker_id, task_name)
        self.logger.debug("%d: PAR: %s", self.worker_id, params)
        return task_name, params

    def process_messages(self) -> None:
        while True:
            msg = self.pubsub.get_message(ignore_subscribe_messages=True)
            if msg is None:
                break
            if msg["channel"].decode("utf-8") not in [
                f"{self.service_name}:{self.worker_id}",
                f"{self.service_name}:0",
            ]:
                continue
            if msg["type"] != "message":
                continue
            cmd, params = self.parse_message(msg)
            if cmd == "exit":
                self._exit = True
            elif cmd == "delay" and len(params) > 0:
                try:
                    self.delay = float(params[0])
                    self.redis_cli.hset(
                        f"{self.service_name}:{self.worker_id}",
                        "delay",
                        value=str(self.delay),
                    )
                except (TypeError, ValueError, IndexError):
                    self.logger.warning(
                        "%d: Wrong argument for delay received", self.worker_id
                    )
            else:
                if not self.execute_cmd(cmd, params):
                    self.logger.warning(
                        "%d: Command %s can not be executed", self.worker_id, cmd
                    )

    def process_tasks(self):
        task = self.redis_cli.lpop(self.service_name + "_tasks")
        if task:
            task_name, params = self.parse_task_data(task.decode("utf-8"))
            if not self.run_task(task_name, params):
                self.logger.warning(
                    "%d: Task %s can not be executed", self.worker_id, task_name
                )

    def execute_cmd(self, cmd: str, params: list[str]):
        """
        Execute command received.
        :param cmd: Command to be executed.
        :param params: list of parameters for the command.
        :return: True if command execution was successful otherwise false.
        """
        self.logger.debug("%d: CMD: %s, PAR: %s", self.worker_id, cmd, params)
        return False

    def run_task(self, task_name: str, params: list[str]):
        """
        Execute command received.
        :param task_name: Task to name to run.
        :param params: list of parameters for the task.
        :return: True if command execution was successful otherwise false.
        """
        self.logger.debug("%d: TASK: %s, PAR: %s", self.worker_id, task_name, params)
        return False

    def initialize(self):
        self.redis_cli.hset(self.service_name, "running", value="1")

    def cleanup(self):
        self.redis_cli.hset(self.service_name, "running", value="0")
        self.redis_cli.close()

    def create_time_series_channel(
        self,
        label: str,
        retention: int = 2_592_000,  # 30 days
        aggregation: Union[tuple[int], list[int], None] = None,
    ):
        """
        Creates time series channel with the given label and retention time.
        If aggregation times are provided as an iterable of numeric values in seconds
        method also creates average and standard deviation aggregation channels
        and sets the rules for them in redis.
        Aggregation channels are named using the following pattern:
            label_avg_60s or label_std.s_30s
        where label is the channel's label and the last part of the name is the aggregation time.

        :param label: The label of the channel.
        :param retention: Retention time in seconds, defaults to 30 days.
        :param aggregation: An optional iterable of aggregation time values in seconds.
        """
        retention_ms = int(max(0, retention * 1000))
        try:
            self.ts.create(
                label,
                retention_msecs=retention_ms,
                labels={"name": self.service_name, "type": "src"},
            )
        except redis.exceptions.ResponseError:
            pass
        self._get_ts_labels()
        if isinstance(aggregation, (list, tuple)):
            for aggr_t in aggregation:
                self.add_time_series_aggregation(label, aggr_t, retention)

    def del_time_series_channel(self, label: str) -> None:
        """Delete time series and all its aggregations by label"""
        if label in self.__ts_labels:
            ts_info = self.ts.info(label)
            for rule in ts_info.rules:
                self.redis_cli.delete(rule[0].decode("utf-8"))
            self.redis_cli.delete(label)
            self._get_ts_labels()

    def add_time_series_aggregation(
        self,
        label: str,
        aggregation: int = 10,  # seconds
        retention: int = 2_592_000,  # seconds, default value is 30 days
    ) -> None:
        """
        Creates average and standard deviation aggregation channels for given channel label
        and sets the rules for them in redis.
        Aggregation channels are named using the following pattern:
            label_avg_60s or label_std_30s
        where label is the channel label and the last part of the name is the aggregation time.

        :param label: The label of the channel.
        :param aggregation: Aggregation time in seconds.
        :param retention: Retention time in seconds, defaults to 30 days.
        """
        if label in self.__ts_labels:
            retention_ms = int(max(0, retention)) * 1000
            aggregation = int(max(0, aggregation))
            aggr_retention_ms = retention_ms * max(1, aggregation)
            aggregation_ms = int(max(0, aggregation)) * 1000
            try:
                for fun in ("avg", "std.s"):
                    self.ts.create(
                        f"{label}_{fun}_{aggregation}s",
                        retention_msecs=aggr_retention_ms,
                        labels={"name": self.service_name, "type": fun},
                    )
                    # Create averaging rule
                    self.ts.createrule(
                        label,
                        f"{label}_{fun}_{aggregation}s",
                        fun,
                        bucket_size_msec=aggregation_ms,
                    )
            except redis.exceptions.ResponseError:
                pass

    def del_time_series_aggregation(self, label: str, aggr_t: int) -> None:
        """Delete time series aggregation"""
        if label in self.__ts_labels:
            for fun in ("avg", "std.s"):
                aggr_label = f"{label}_{fun}_{aggr_t}s"
                self.redis_cli.delete(aggr_label)

    def put_ts_data(
        self, label: str, value: float, timestamp_ms: Union[int, None] = None
    ):
        """Puts data to redis time series channel"""
        if timestamp_ms is None:
            timestamp_ms = time_ms()
        self.ts.add(
            label,
            timestamp_ms,
            value,
            labels={"name": self.service_name, "type": "src"},
        )
