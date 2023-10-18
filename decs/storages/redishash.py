import redis

from decs.abc import DECS_PREFIX, BaseEntity, BaseComponentStorage, ComponentTypeDescriptor
from decs.serialization import get_component_name, packb, unpackb
from decs.storages.redis import RedisComponentStorage, Serializer, Deserializer


class RedisHashStorage(RedisComponentStorage):
    def __init__(
        self,
        component_type: ComponentTypeDescriptor,
        client: redis.Redis,
        ttl: int | None = None,
        serialize: Serializer = packb,
        deserialize: Deserializer = unpackb,
    ):
        super().__init__(
            component_type,
            client,
            ttl,
            serialize,
            deserialize,
        )

    def _get_key_for_entity(self, entity: BaseEntity) -> str:
        return f"{DECS_PREFIX}:component:{get_component_name(self._component_type)}:{entity.uid()}"
