from collections.abc import Callable
from functools import wraps
from typing import Any


class RegisterManager:
    """Class-based decorator with proper type hints"""

    def __init__(self, name: str | None = None):
        self.name = name
        self._is_decorated = True  # Attribute is properly defined

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self_up: MyClass, *args, **kwargs) -> Callable:
            if hasattr(func, "__name__"):
                func_name = str(func.__name__)
            elif hasattr(func, "__class__"):
                func_name = func.__class__.__name__
            else:
                func_name = str(func)
            self.name: str = self.name or func_name

            def my_fun(self_f: MyClass, *args, **kwargs):
                print(f"[{my_fun.__name__}] Calling `{self.name}` on `{self_f.name}`")
                return func(self, *args, **kwargs)

            self_up.decorated[self.name] = my_fun
            return my_fun

        # Type checker sees this as an attribute of the wrapper
        # wrapper = update_wrapper(wrapper, func)
        setattr(wrapper, "_is_decorated", True)  # Dynamic attribute
        return wrapper


class MyClass:
    def __init__(self, name):
        self.name = name
        self.decorated: dict[str, Any] = {}
        self._init_managers()

    def _init_managers(self):
        """Count decorated methods in this class"""
        count = 0
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, "_is_decorated"):
                attr()
                count += 1
        print("TOTAL:", count)
        i = 1
        for fun in self.decorated:
            print(i, fun, self.decorated[fun])
            print(">> CALL")
            self.decorated[fun](self)
            print()
            i += 1
        return count

    @RegisterManager()
    def say_hello(self):
        print(f"Hello, I'm {self.name}")

    @RegisterManager()
    def say_goodbye(self):
        print(f"Goodbye from {self.name}")

    @RegisterManager()
    def say_hi(self):
        print(f"Hi, I'm {self.name}")


if __name__ == "__main__":
    obj = MyClass("Alice")
