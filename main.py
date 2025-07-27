import asyncio
import enum
from typing import Annotated, Callable, get_type_hints, get_origin, get_args

from contextlib import asynccontextmanager


_sentinel = ...


class Scope(enum.Enum):
    request: str = "request"
    singleton: str = "singleton"


class Container:
    def register(self, **kwargs):
        """Register values dynamically into container fields"""
        for key, value in kwargs.items():
            if not hasattr(self.__class__, key):
                raise AttributeError(f"No injectable field named '{key}'")
            injected_value = getattr(self, key)
            injected_value.register(value)


def _get_dependencies(fn: Callable) -> list:
    injected_args = []
    hints = get_type_hints(fn, include_extras=True)
    for func_param_name, hint in hints.items():
        if get_origin(hint) is not Annotated:
            continue
        _, *extras = get_args(hint)
        for extra in extras:
            if not isinstance(extra, Inject):
                continue
            key = extra.key or func_param_name
            injected_args.append((func_param_name, key))
    return injected_args


class Injector:
    def __init__(self, fabric, container):
        self.dependencies = _get_dependencies(fabric)
        self.container = container

    async def __aenter__(self):
        resolve_dependencies = {
            param_name: asyncio.create_task(getattr(self.container, key).__aenter__())
            for key, param_name in self.dependencies
        }
        return {key: await value for key, value in resolve_dependencies.items()}

    async def __aexit__(self, *args, **kwargs):
        await asyncio.gather(
            *(
                getattr(self.container, key).__aexit__(*args, **kwargs)
                for key, _ in self.dependencies
            )
        )


class Dependency:
    def __init__(self, fabric=None, default=None):
        self._fabric = fabric
        self._default = default
        self._value = None
        self._injector = _sentinel

    def __set_name__(self, obj, name):
        if self._fabric:
            self._injector = Injector(fabric=self._fabric, container=obj)

    def register(self, value):
        self._value = value

    async def __aenter__(self):
        static_value = self._value or self._default
        if static_value is not None:
            self._value = static_value
            return self._value

        kwargs = await self._injector.__aenter__()
        self.context_wrapper = asynccontextmanager(self._fabric)(**kwargs)
        return await self.context_wrapper.__aenter__()

    async def __aexit__(self, *args, **kwargs):
        if self._value is None:
            await self._injector.__aexit__(*args, **kwargs)
            await self.context_wrapper.__aexit__(*args, **kwargs)


class Inject:
    """Marker used in Annotated to declare injected parameters"""

    def __init__(self, key: str = None):
        self.key = key
