import uuid
from typing import Callable, Generic, Iterable, Iterator, Type, TypeVar
from abc import ABC, abstractmethod

from channels_redis.pubsub import RedisPubSubChannelLayer
from pydantic import BaseModel


Component = TypeVar("Component", bound=BaseModel)
ComponentType = Type[Component]
ComponentTypeDescriptor = str | ComponentType

DECS_PREFIX = "__decs"

Serializer = Callable[[Component], bytes]
Deserializer = Callable[[bytes, ComponentType], Component]

ChannelLayer = TypeVar("ChannelLayer", bound=RedisPubSubChannelLayer)


class BaseEntity(ABC):
    _uid: uuid.UUID | None
    _backend: "BaseBackend"

    def __init__(
        self,
        backend: "BaseBackend",
        uid: uuid.UUID | None = None,
    ):
        self._uid = uid
        self._backend = backend

    @staticmethod
    @abstractmethod
    def from_uid(
        uid: uuid.UUID,
        backend: "BaseBackend",
    ):
        ...

    def is_null(self) -> bool:
        return self._uid is None

    @abstractmethod
    def has(
        self,
        component_type: str | Type[BaseModel],
    ) -> bool:
        ...

    @abstractmethod
    def uid(self):
        ...

    @abstractmethod
    def version(self):
        ...

    @abstractmethod
    def get(
        self,
        component_type: ComponentTypeDescriptor,
    ) -> Component:
        ...

    def get_many(self, *component_types: ComponentTypeDescriptor) -> Iterable[Component]:
        for component_type in component_types:
            yield self.get(component_type)

    @abstractmethod
    def set(
        self,
        value: Component,
        component_type: ComponentTypeDescriptor | None = None,
        **kwargs,
    ):
        ...

    def set_many(
        self,
        *values: Component,
        component_types: ComponentTypeDescriptor
        | list[ComponentTypeDescriptor]
        | tuple[ComponentTypeDescriptor]
        | None = None,
        **kwargs,
    ):
        if component_types is None:
            component_types = [None] * len(values)

        for value, component_type in zip(values, component_types):
            self.set(value, component_type, **kwargs)

    @abstractmethod
    def remove(self, component_type: ComponentTypeDescriptor):
        ...

    def remove_many(self, *component_types: ComponentTypeDescriptor):
        for component_type in component_types:
            self.remove(component_type)

    def __getitem__(
        self,
        component_type: ComponentTypeDescriptor | list[ComponentTypeDescriptor] | tuple[ComponentTypeDescriptor],
    ):
        if isinstance(component_type, (list, tuple)):
            return self.get_many(*component_type)

        return self.get(component_type)

    def __setitem__(
        self,
        value: Component,
        component_type: ComponentTypeDescriptor | None = None,
    ):
        self.set(value, component_type)

    def __delitem__(
        self,
        component_type: ComponentTypeDescriptor | list[ComponentTypeDescriptor] | tuple[ComponentTypeDescriptor],
    ):
        if isinstance(component_type, (list, tuple)):
            self.remove_many(*component_type)
        else:
            self.remove(component_type)

    def __hash__(self):
        return hash(self._uid)

    @abstractmethod
    def __eq__(self, other: "BaseEntity"):
        ...


class BaseComponentStorage(ABC, Generic[Component], Iterable[BaseEntity]):
    _component_type: ComponentType

    def __init__(self, component_type: ComponentType):
        self._component_type = component_type

    def component_type(self) -> ComponentType:
        return self._component_type

    @abstractmethod
    def get(self, entity: BaseEntity) -> Component | None:
        ...

    def get_many(self, entities: Iterable[Component]) -> Iterable[Component | None]:
        for entity in entities:
            yield self.get(entity)

    @abstractmethod
    def set(
        self,
        entity: BaseEntity,
        value: Component,
        ttl: int | None = None,
    ):
        ...

    def set_many(
        self,
        entities: Iterable[BaseEntity],
        values: Iterable[Component],
        ttl: int | None = None,
    ):
        for entity, value in zip(entities, values):
            self.set(entity, value, ttl)

    @abstractmethod
    def remove(self, entity: BaseEntity):
        ...

    def remove_many(self, entities: Iterable[BaseEntity]):
        for entity in entities:
            self.remove(entity)

    @abstractmethod
    def keys(self) -> Iterable[BaseEntity]:
        ...

    @abstractmethod
    def values(self) -> Iterable[Component]:
        ...

    @abstractmethod
    def items(self) -> Iterable[tuple[BaseEntity, Component]]:
        ...

    @abstractmethod
    def clear(self):
        ...

    @abstractmethod
    def has(self, entity: BaseEntity) -> bool:
        ...

    def __contains__(self, entity: BaseEntity) -> bool:
        return self.has(entity)

    @abstractmethod
    def __len__(self) -> int:
        ...

    @abstractmethod
    def __iter__(self) -> Iterator[BaseEntity]:
        ...


class BaseRegistry(ABC):
    pass


class BaseBackend(ABC):
    @abstractmethod
    def entity(self, uid: uuid.UUID | None = None) -> BaseEntity:
        ...

    @abstractmethod
    def has_entity(self, uid: uuid.UUID) -> bool:
        ...

    @abstractmethod
    def resolve_component_type(
        self,
        component_type: ComponentTypeDescriptor,
    ) -> ComponentType:
        ...

    @abstractmethod
    def get_component_storage(
        self,
        component_type: ComponentTypeDescriptor,
    ) -> BaseComponentStorage:
        ...

    @abstractmethod
    def get_registry(
        self,
        *component_types: ComponentTypeDescriptor,
    ) -> BaseRegistry:
        ...

    @abstractmethod
    def clear(self):
        ...

    @abstractmethod
    def _get_storages(self) -> Iterable[BaseComponentStorage]:
        ...
