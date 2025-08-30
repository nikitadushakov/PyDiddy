import asyncio
from typing import Any, Annotated, Callable, get_origin, get_args
from contextlib import asynccontextmanager
import inspect


_sentinel: Any = object()


class Inject:
    """Marker used in Annotated to declare injected parameters"""

    def __init__(self, key: str | None = None):
        self.key = key


class FabricIsNoDefined(Exception): ...


class Container:
    dependencies = {}

    @classmethod
    def get_dependencies(cls, fn: Callable) -> dict:
        injected_args = {}
        parameters = inspect.signature(fn).parameters
        for parametr in parameters:
            annotation = parameters[parametr].annotation
            if get_origin(annotation) is not Annotated:
                continue
            _, extra = get_args(annotation)
            if not isinstance(extra, Inject):
                continue
            key = extra.key or parametr
            injected_args[parametr] = getattr(cls, key)
        return injected_args

    @classmethod
    @asynccontextmanager
    async def inject(cls, fn, *args, **kwargs):
        injected_dependencies = []
        dependencies = cls.get_dependencies(fn)
        trb = exc = exc_type = None
        try:
            resolved_dependencies = [
                (param_name, dependency.provide(kwargs.get(param_name, _sentinel)))
                for param_name, dependency in dependencies.items()
            ]
            param_names, dependencies = zip(*resolved_dependencies)
            kwargs = dict(zip(param_names, await asyncio.gather(*dependencies)))
            print("successfull prepare dependencies")
            yield args, kwargs
        except Exception as e:
            exc = e
            exc_type = type(exc)
            trb = exc.__traceback__
        finally:
            print("close dependencies....")
            await asyncio.gather(
                *(
                    dependencies[param_name].close(exc, exc_type, trb)
                    for param_name in injected_dependencies
                )
            )

    def register(self, **kwargs):
        """Register values dynamically into container fields"""
        for key, value in kwargs.items():
            if not hasattr(self.__class__, key):
                raise AttributeError(f"No dependency named '{key}'")
            dependency = getattr(self, key)
            dependency.register(value)


def inject(container):
    def wrapper(fn):
        async def async_wrapper(*args, **kwargs):
            async with container.inject(fn, *args, **kwargs) as (args, kwargs):
                res = await fn(*args, **kwargs)
            return res

        return async_wrapper

    return wrapper


class BaseDependencyProvider:
    def __init__(self, dependency: "Dependency"):
        self._dependency = dependency
        self._container = _sentinel

    def set_container(self, container):
        self._container = container

    async def provide(self):
        raise NotImplementedError

    async def close(self, exc, exc_type, trb) -> None:
        raise NotImplementedError

    async def __aenter__(self):
        return await self.provide()

    async def __aexit__(self, exc, exc_type, trb) -> None:
        await self.close(exc, exc_type, trb)


class ValueDependencyProvider(BaseDependencyProvider):
    async def provide(self):
        print("provide value..")
        return self._dependency._default

    async def close(self, exc, exc_type, trb): ...


class SingletonDependencyProvider(BaseDependencyProvider):
    async def provide(self):
        self._dependency_wrapper = self._container.inject(self._dependency._fabric)
        args, kwargs = await self._dependency_wrapper.__aenter__()
        print("provide singleton...")
        return await self._dependency._fabric(*args, **kwargs)

    async def close(self, exc, exc_type, trb):
        await self._dependency_wrapper.__aexit__(exc, exc_type, trb)


class RequestDependencyProvider(BaseDependencyProvider):
    def __init__(self, dependency: "Dependency"):
        super().__init__(dependency)
        self._context_wrapper = asynccontextmanager(self._dependency._fabric)

    async def provide(self):
        self._dependency_wrapper = self._container.inject(self._dependency._fabric)
        args, kwargs = await self._dependency_wrapper.__aenter__()
        self._request_obj = self._context_wrapper(*args, **kwargs)
        return await self._request_obj.__aenter__()

    async def close(self, exc, exc_type, trb):
        await self._request_obj.__aexit__(exc, exc_type, trb)
        await self._dependency_wrapper.__aexit__(exc, exc_type, trb)


class Dependency:
    """top-level class for store dependency"""

    def __init__(self, fabric=_sentinel, default=_sentinel):
        self._fabric = fabric
        self._default = default
        self._value = _sentinel
        self.provider = self.get_provider()

    def __set_name__(self, obj: Container, name: str):
        obj.dependencies[name] = self
        self.provider.set_container(obj)

    def register(self, value):
        self._value = value

    def get_provider(self) -> BaseDependencyProvider:
        if self._fabric is _sentinel:
            return ValueDependencyProvider(dependency=self)
        elif inspect.isasyncgenfunction(self._fabric):
            return RequestDependencyProvider(dependency=self)
        elif inspect.iscoroutinefunction(self._fabric):
            return SingletonDependencyProvider(dependency=self)
        raise FabricIsNoDefined(self._fabric)

    async def provide(self, existing_value):
        if existing_value is not _sentinel:
            return existing_value

        if self._value is not _sentinel:
            return self._value
        value = await self.provider.provide()
        self.register(value)
        return self._value

    async def close(self, exc, exc_type, trb):
        if self._value is not _sentinel:
            self._value = _sentinel
            await self.provider.close(exc, exc_type, trb)
