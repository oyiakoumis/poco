"""Singleton metaclass for creating singleton classes."""

from typing import Any, Dict, Type


class Singleton(type):
    """Metaclass for creating singleton classes.

    Usage:
        class MyClass(metaclass=Singleton):
            pass
    """

    _instances: Dict[Type, Any] = {}

    def __call__(cls, *args, **kwargs):
        """Ensure only one instance of the class exists."""
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
