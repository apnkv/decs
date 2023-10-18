import enum
from typing import Callable, Type, TypeVar

from pydantic import BaseModel

from decs.schemas import DecsSchema


class DeliveryMode(enum.StrEnum):
    ALL = "all"
    ANY = "any"


Event = TypeVar("Event", bound=BaseModel)
Serializer = Callable[[Event], bytes]
Deserializer = Callable[[bytes, Type[Event]], Event]


class WrappedEvent(DecsSchema):
    event: bytes
    event_type: str
