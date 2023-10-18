import importlib
import uuid
from typing import Callable, Iterable, Type, TypeVar

import redis

from decs.abc import (
    BaseBackend,
    Component,
    ComponentType,
    ComponentTypeDescriptor,
    BaseRegistry,
    BaseComponentStorage,
    BaseEntity,
    DECS_PREFIX,
)
from decs.entity import Entity
from decs.storages.redis import RedisComponentStorage
from decs.schemas import DecsSchema


class RedisBackend(BaseBackend):
    _client: redis.Redis

    def __init__(
        self,
        client: redis.Redis | None = None,
        ttl: int | None = None,
    ):
        super().__init__()

        self._component_storages: dict[Component, RedisComponentStorage] = {}
        self._client = client or redis.Redis(decode_responses=False)
        self._ttl = ttl

    def entity(self, uid: uuid.UUID | None = None) -> Entity:
        entity = Entity.from_uid(uid or uuid.uuid4(), self)
        key = f"{DECS_PREFIX}:entity:{entity.uid()}"

        with self._client.pipeline() as pipe:
            pipe.set(f"{DECS_PREFIX}:entity:{entity.uid()}", 0)
            if self._ttl is not None:
                pipe.expire(key, self._ttl, gt=True)
            pipe.execute()

        return entity

    def has_entity(self, uid: uuid.UUID) -> bool:
        return bool(self._client.exists(f"{DECS_PREFIX}:entity:{uid}"))

    def resolve_component_type(self, component_type: ComponentTypeDescriptor) -> ComponentType | None:
        result = component_type

        if isinstance(component_type, str):
            components = component_type.split(":")
            try:
                module = importlib.import_module(components[0])
                result = getattr(module, components[1])
            except (ModuleNotFoundError, ImportError, AttributeError):
                result = None

        if result is not None and not issubclass(result, DecsSchema):
            print(f"WARNING: Component type {component_type} is not a subclass of DurerSchema")

        return result

    def get_component_storage(self, component_type: ComponentTypeDescriptor) -> BaseComponentStorage | None:
        component_type = self.resolve_component_type(component_type)

        if component_type is not None and component_type not in self._component_storages:
            self._component_storages[component_type] = RedisComponentStorage(
                component_type, self._client, ttl=self._ttl
            )

        if component_type is None:
            return None

        return self._component_storages[component_type]

    def clear(self):
        entity_keys = self._client.keys(f"{DECS_PREFIX}:entity:*")
        component_keys = self._client.keys(f"{DECS_PREFIX}:storages:*")

        keys_to_delete = entity_keys + component_keys
        keys_to_delete = [key.decode("utf-8") for key in keys_to_delete]

        if keys_to_delete:
            self._client.delete(*keys_to_delete)

    def get_registry(self, *component_types: ComponentTypeDescriptor) -> BaseRegistry:
        pass

    def _get_storages(self) -> Iterable[BaseComponentStorage]:
        for storage in self._component_storages.values():
            yield storage
