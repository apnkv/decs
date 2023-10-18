import math
import uuid
from typing import Callable, Iterator, Iterable

import redis

from decs.serialization import get_component_name, packb, unpackb
from decs.abc import (
    BaseBackend,
    BaseComponentStorage,
    BaseEntity,
    ComponentType,
    ComponentTypeDescriptor,
    Component,
    DECS_PREFIX,
)

Serializer = Callable[[Component], bytes]
Deserializer = Callable[[bytes, ComponentType], Component]


class RedisComponentStorage(BaseComponentStorage):
    def __init__(
        self,
        component_type: ComponentTypeDescriptor,
        client: redis.Redis,
        ttl: int | None = None,
        serialize: Serializer = packb,
        deserialize: Deserializer = unpackb,
    ):
        super().__init__(component_type)

        self._client = client
        self._ttl = ttl

        self._serialize = serialize
        self._deserialize = deserialize

    def get(self, entity: BaseEntity) -> Component | None:
        value = self._ensure_bytes(
            self._client.get(
                self._get_key_for_entity(entity),
            ),
        )

        return self._deserialize(value, self._component_type)

    def set(self, entity: BaseEntity, value: Component, ttl: int | None = None):
        if entity.is_null():
            raise ValueError("Cannot set component for null entity")

        self._client.set(
            self._get_key_for_entity(entity),
            self._serialize(value),
            ex=ttl or self._ttl,
        )

    def set_many(
        self,
        entities: Iterable[BaseEntity],
        values: Iterable[Component],
        ttl: int | None = None,
    ):
        items = dict()
        for entity, value in zip(entities, values):
            if entity.is_null():
                raise ValueError("Cannot set component for null entity")

            items[self._get_key_for_entity(entity)] = self._serialize(value)
        self._client.mset(items)

        if ttl or self._ttl:
            pipe = self._client.pipeline()
            for entity in entities:
                pipe.expire(self._get_key_for_entity(entity), ttl or self._ttl)
            pipe.execute()

    def remove(self, entity: BaseEntity):
        self._client.delete(self._get_key_for_entity(entity))

    def remove_many(self, entities: Iterable[BaseEntity]):
        self._client.delete(*[self._get_key_for_entity(entity) for entity in entities])

    def keys(self, backend: BaseBackend = None) -> Iterable[uuid.UUID | BaseEntity]:
        for key in self._client.scan_iter(self._get_wildcard_key()):
            uid = uuid.UUID(key.decode("utf-8").rsplit(":", 1)[1])
            if backend is not None:
                yield backend.entity(uid)

            yield uid

    def values(self) -> Iterable[Component]:
        for key in self._client.scan_iter(self._get_wildcard_key()):
            value = self._ensure_bytes(self._client.get(key))
            yield self._deserialize(value, self._component_type)

    def items(self) -> Iterable[tuple[BaseEntity, Component]]:
        pass

    def clear(self):
        keys = self._client.keys(self._get_wildcard_key())
        if keys:
            self._client.delete(*keys)

    def has(self, entity: BaseEntity) -> bool:
        if entity.is_null():
            return False

        return bool(
            self._client.exists(
                self._get_key_for_entity(entity),
            )
        )

    def __len__(self) -> int:
        pass

    def __iter__(self) -> Iterator[BaseEntity]:
        pass

    @staticmethod
    def _ensure_bytes(value: bytes | str) -> bytes:
        if isinstance(value, str):
            value = value.encode("utf-8")
        return value

    def _get_wildcard_key(self) -> str:
        print(f"{DECS_PREFIX}:storages:{get_component_name(self._component_type)}:*", flush=True)
        return f"{DECS_PREFIX}:storages:{get_component_name(self._component_type)}:*"

    def _get_key_for_entity(self, entity: BaseEntity) -> str:
        return f"{DECS_PREFIX}:storages:{get_component_name(self._component_type)}:{entity.uid()}"
