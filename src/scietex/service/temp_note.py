import asyncio
import functools
from typing import Dict, Optional, Callable, Any
from enum import Enum
import logging


class ManagerStatus(Enum):
    RUNNING = "running"
    CLEANING = "cleaning"
    DONE = "done"
    CANCELLED = "cancelled"
    ERROR = "error"


class UnifiedManager:
    """
    Универсальный класс, который одновременно:
    1. Содержит методы-менеджеры
    2. Управляет их жизненным циклом
    3. Отслеживает состояние
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._statuses: Dict[str, ManagerStatus] = {}
        self._results: Dict[str, Any] = {}
        self._errors: Dict[str, Exception] = {}
        self._cleanup_functions: Dict[str, Callable] = {}
        self._logger = logger or logging.getLogger(__name__)

    # ==================== Декораторы ====================

    def manager(self, name: Optional[str] = None, cleanup: Optional[Callable] = None):
        """
        Декоратор для регистрации метода как менеджера.

        Args:
            name: Имя менеджера (если не указано, используется имя метода)
            cleanup: Функция очистки, вызываемая при отмене или ошибке
        """

        def decorator(method: Callable) -> Callable:
            manager_name = name or method.__name__
            self._cleanup_functions[manager_name] = cleanup

            @functools.wraps(method)
            async def wrapper(self_instance, *args, **kwargs):
                self._statuses[manager_name] = ManagerStatus.RUNNING
                self._errors[manager_name] = None

                self._logger.info(f"🟢 Менеджер {manager_name} запущен")

                try:
                    result = await method(self_instance, *args, **kwargs)
                    self._results[manager_name] = result
                    self._statuses[manager_name] = ManagerStatus.DONE
                    self._logger.info(f"✅ Менеджер {manager_name} успешно завершён")
                    return result

                except asyncio.CancelledError:
                    self._statuses[manager_name] = ManagerStatus.CANCELLED
                    self._logger.info(f"🟡 Менеджер {manager_name} отменён")

                    # Выполняем очистку
                    await self._run_cleanup(manager_name)
                    raise

                except Exception as e:
                    self._statuses[manager_name] = ManagerStatus.ERROR
                    self._errors[manager_name] = e
                    self._logger.error(f"🔴 Менеджер {manager_name} ошибка: {e}")

                    # Выполняем очистку
                    await self._run_cleanup(manager_name)
                    raise

            wrapper._is_manager = True
            wrapper._manager_name = manager_name
            return wrapper

        return decorator

    async def _run_cleanup(self, manager_name: str):
        """Выполняет функцию очистки для менеджера"""
        cleanup_func = self._cleanup_functions.get(manager_name)
        if cleanup_func:
            try:
                self._statuses[manager_name] = ManagerStatus.CLEANING
                await cleanup_func(self)
                self._logger.info(f"🧹 Очистка {manager_name} завершена")
            except Exception as e:
                self._logger.error(f"❌ Ошибка очистки {manager_name}: {e}")

    # ==================== Управление задачами ====================

    def start(self, method_name: str, *args, task_name: Optional[str] = None, **kwargs):
        """
        Запускает метод как задачу.

        Args:
            method_name: Имя метода-менеджера
            *args: Аргументы для метода
            task_name: Имя задачи (если не указано, используется имя менеджера)
            **kwargs: Ключевые аргументы для метода
        """
        method = getattr(self, method_name)
        if not hasattr(method, "_is_manager"):
            raise ValueError(f"{method_name} не зарегистрирован как менеджер")

        manager_name = task_name or method._manager_name

        # Проверяем, не запущена ли уже задача
        if manager_name in self._tasks and not self._tasks[manager_name].done():
            raise RuntimeError(f"Менеджер {manager_name} уже запущен")

        # Создаём задачу
        task = asyncio.create_task(method(*args, **kwargs), name=manager_name)
        self._tasks[manager_name] = task

        # Добавляем callback для автоматического обновления статуса
        def update_status(task):
            if task.cancelled():
                self._statuses[manager_name] = ManagerStatus.CANCELLED
            elif task.exception():
                self._statuses[manager_name] = ManagerStatus.ERROR
                self._errors[manager_name] = task.exception()
            else:
                self._statuses[manager_name] = ManagerStatus.DONE
                try:
                    self._results[manager_name] = task.result()
                except Exception:
                    pass
            self._logger.debug(
                f"Статус {manager_name} обновлён: {self._statuses[manager_name].value}"
            )

        task.add_done_callback(update_status)
        return task

    async def stop(self, name: str, timeout: float = 5.0, force: bool = False):
        """
        Останавливает задачу.

        Args:
            name: Имя задачи
            timeout: Таймаут ожидания завершения
            force: Если True, отменяет задачу даже если она выполняется
        """
        task = self._tasks.get(name)
        if not task:
            self._logger.warning(f"Задача {name} не найдена")
            return False

        if task.done():
            self._logger.info(f"Задача {name} уже завершена")
            return True

        if not force:
            self._logger.warning(f"Задача {name} всё ещё выполняется")
            return False

        self._logger.info(f"⏹️ Останавливаю {name}...")
        task.cancel()

        try:
            await asyncio.wait_for(task, timeout=timeout)
            self._logger.info(f"✅ Задача {name} остановлена")
            return True
        except asyncio.TimeoutError:
            self._logger.error(f"❌ Таймаут остановки {name} за {timeout} секунд")
            return False
        except asyncio.CancelledError:
            self._logger.info(f"✅ Задача {name} отменена")
            return True

    async def stop_all(self, timeout: float = 5.0):
        """Останавливает все задачи"""
        if not self._tasks:
            return

        self._logger.info(f"⏹️ Останавливаю все задачи ({len(self._tasks)})")

        # Отменяем все задачи
        for name, task in self._tasks.items():
            if not task.done():
                task.cancel()

        # Ждём завершения
        tasks = list(self._tasks.values())
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)
        except asyncio.TimeoutError:
            self._logger.error(f"❌ Таймаут остановки всех задач за {timeout} секунд")

        self._tasks.clear()
        self._logger.info("✅ Все задачи остановлены")

    # ==================== Методы получения информации ====================

    def get_status(self, name: str) -> Optional[ManagerStatus]:
        """Возвращает статус менеджера"""
        return self._statuses.get(name)

    def is_running(self, name: str) -> bool:
        """Проверяет, выполняется ли задача"""
        task = self._tasks.get(name)
        return task is not None and not task.done()

    def get_result(self, name: str) -> Any:
        """Возвращает результат менеджера"""
        return self._results.get(name)

    def get_error(self, name: str) -> Optional[Exception]:
        """Возвращает ошибку менеджера"""
        return self._errors.get(name)

    def get_all_managers(self) -> Dict[str, Dict]:
        """Возвращает информацию о всех менеджерах"""
        return {
            name: {
                "status": self._statuses.get(name),
                "running": self.is_running(name),
                "result": self._results.get(name),
                "error": self._errors.get(name),
                "has_cleanup": name in self._cleanup_functions,
            }
            for name in self._cleanup_functions.keys()
        }

    # ==================== Примеры методов-менеджеров ====================

    @manager(name="worker1", cleanup=lambda self: self._cleanup_worker1())
    async def worker1(self, duration: int = 10):
        """Первый работник - имитация работы с БД"""
        self._logger.info("Worker1: Подключаюсь к БД...")
        await asyncio.sleep(0.5)

        for i in range(duration):
            await asyncio.sleep(1)
            self._logger.info(f"Worker1: Обработка записи {i + 1}")

            # Имитация потенциальной проблемы
            if i == 3:
                self._logger.warning("Worker1: Проблема с соединением!")
                # Но продолжаем работу

            # Имитация длительной операции
            if i % 2 == 0:
                await asyncio.sleep(0.2)

        self._logger.info("Worker1: Завершаю работу с БД")
        return {"status": "success", "records": duration}

    async def _cleanup_worker1(self):
        """Очистка для worker1"""
        self._logger.info("🧹 Worker1: Закрываю соединение с БД...")
        await asyncio.sleep(0.5)
        self._logger.info("🧹 Worker1: БД закрыта")

    @manager(name="worker2", cleanup=lambda self: self._cleanup_worker2())
    async def worker2(self, duration: int = 8):
        """Второй работник - имитация работы с файлами"""
        self._logger.info("Worker2: Открываю файлы...")
        files = [f"file_{i}.txt" for i in range(3)]

        for i in range(duration):
            await asyncio.sleep(1.5)
            self._logger.info(f"Worker2: Запись в {files[i % len(files)]} (шаг {i + 1})")

            # Имитация ошибки
            if i == 5:
                raise ValueError("Worker2: Ошибка записи в файл!")

        self._logger.info("Worker2: Завершаю работу с файлами")
        return {"status": "success", "files": files}

    async def _cleanup_worker2(self):
        """Очистка для worker2"""
        self._logger.info("🧹 Worker2: Закрываю файлы...")
        await asyncio.sleep(0.3)
        self._logger.info("🧹 Worker2: Файлы закрыты")

    @manager(name="worker3")
    async def worker3(self, duration: int = 5):
        """Третий работник - без очистки"""
        self._logger.info("Worker3: Простой работник")
        for i in range(duration):
            await asyncio.sleep(0.5)
            self._logger.info(f"Worker3: Шаг {i + 1}")
        return {"status": "success", "steps": duration}

    # ==================== Дополнительные утилиты ====================

    async def run_workers_parallel(self, workers: Dict[str, Dict] = None):
        """
        Запускает несколько работников параллельно.

        Args:
            workers: Словарь {имя_метода: {args, kwargs, task_name}}
        """
        if workers is None:
            # По умолчанию запускаем всех воркеров с параметрами по умолчанию
            workers = {
                "worker1": {"kwargs": {"duration": 10}},
                "worker2": {"kwargs": {"duration": 8}},
                "worker3": {"kwargs": {"duration": 5}},
            }

        tasks = []
        for method_name, params in workers.items():
            args = params.get("args", ())
            kwargs = params.get("kwargs", {})
            task_name = params.get("task_name", method_name)

            task = self.start(method_name, *args, task_name=task_name, **kwargs)
            tasks.append(task)

        return tasks

    async def wait_for_completion(self, names: Optional[List[str]] = None):
        """
        Ожидает завершения указанных задач.

        Args:
            names: Список имён задач (если None, ждёт все)
        """
        tasks_to_wait = []
        if names:
            tasks_to_wait = [self._tasks[name] for name in names if name in self._tasks]
        else:
            tasks_to_wait = list(self._tasks.values())

        if tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)

    async def restart(self, name: str, *args, **kwargs):
        """
        Перезапускает менеджер.

        Args:
            name: Имя менеджера
            *args, **kwargs: Аргументы для метода
        """
        # Останавливаем
        await self.stop(name, force=True)

        # Ждём немного
        await asyncio.sleep(0.1)

        # Запускаем заново
        return self.start(name, *args, **kwargs)


# ==================== Использование ====================


async def main():
    # Создаём экземпляр
    manager = UnifiedManager()

    # Настраиваем логирование
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    print("=" * 50)
    print("Запускаем менеджеры...")
    print("=" * 50)

    # Вариант 1: Запуск отдельных методов
    task1 = manager.start("worker1", duration=6, task_name="DB_Worker")
    task2 = manager.start("worker2", duration=4, task_name="File_Worker")
    task3 = manager.start("worker3", duration=3, task_name="Simple_Worker")

    # Даём поработать
    await asyncio.sleep(2)

    # Проверяем статусы
    print("\n" + "=" * 50)
    print("Статусы менеджеров:")
    print("=" * 50)
    for name in ["DB_Worker", "File_Worker", "Simple_Worker"]:
        status = manager.get_status(name)
        running = manager.is_running(name)
        print(f"{name}: {status.value if status else 'unknown'} (running: {running})")

    # Останавливаем один из них
    print("\n" + "=" * 50)
    print("Останавливаем DB_Worker...")
    print("=" * 50)
    await manager.stop("DB_Worker", force=True)

    # Даём поработать остальным
    await asyncio.sleep(2)

    # Останавливаем все
    print("\n" + "=" * 50)
    print("Останавливаем все...")
    print("=" * 50)
    await manager.stop_all(timeout=3.0)

    # Показываем финальные результаты
    print("\n" + "=" * 50)
    print("Финальные результаты:")
    print("=" * 50)
    all_managers = manager.get_all_managers()
    for name, info in all_managers.items():
        print(f"\n{name}:")
        print(f"  Статус: {info['status'].value if info['status'] else 'unknown'}")
        print(f"  Результат: {info['result']}")
        if info["error"]:
            print(f"  Ошибка: {info['error']}")
        print(f"  Очистка: {'есть' if info['has_cleanup'] else 'нет'}")

    # Пример перезапуска
    print("\n" + "=" * 50)
    print("Перезапускаем worker3...")
    print("=" * 50)
    await manager.restart("worker3", duration=2)
    await asyncio.sleep(3)

    # Останавливаем всё
    await manager.stop_all()

    print("\n✅ Программа завершена")


if __name__ == "__main__":
    asyncio.run(main())
