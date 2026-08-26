from functools import wraps
from typing import Any


def log_call(name: str | None = None):  # This is the decorator factory

    def decorator(func):  # This is the actual decorator

        @wraps(func)
        def wrapper(self_up: MyClass):

            func_name: str = name or func.__name__

            def my_fun(self: MyClass, *args, **kwargs):
                print(f"[{func_name}] Calling {func.__name__} on {self.name}")
                return func(self, *args, **kwargs)

            self_up.decorated[func_name] = my_fun
            return my_fun

        setattr(wrapper, "_is_decorated", True)  # Dynamic attribute
        return wrapper

    return decorator


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

    @log_call()  # ✅ Works with argument
    def say_hello(self):
        print(f"Hello, I'm {self.name}")

    @log_call()  # ✅ Different argument
    def say_goodbye(self):
        print(f"Goodbye from {self.name}")

    @log_call()  # ✅ Uses default "LOG"
    def say_hi(self):
        print(f"Hi, I'm {self.name}")


if __name__ == "__main__":
    obj = MyClass("Alice")
    # obj.say_hello()  # [DEBUG] Calling say_hello on Alice\nHello, I'm Alice
    # obj.say_goodbye()  # [ERROR] Calling say_goodbye on Alice\nGoodbye from Alice
    # obj.say_hi()  # [LOG] Calling say_hi on Alice\nHi, I'm Alice
