from typing import Optional

from smallpond.platform.base import Platform
from smallpond.platform.mpi import MPI

_platforms = {
    "mpi": MPI,
}


def get_platform(name: Optional[str] = None) -> Platform:
    """
    Get a platform by name.
    If name is not specified, try to get an available platform.
    """
    if name is None:
        for platform in _platforms.values():
            if platform.is_available():
                return platform()
        return Platform()

    if name in _platforms:
        return _platforms[name]()

    # load platform from a custom python module
    from importlib import import_module

    module = import_module(name)

    # find the exact class that inherits from Platform
    for name in dir(module):
        cls = getattr(module, name)
        if isinstance(cls, type) and issubclass(cls, Platform):
            return cls()

    raise RuntimeError(f"no Platform class found in module: {name}")
