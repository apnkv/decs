import uuid
from datetime import datetime, timezone
from pydantic import Field
from typing import List

import numpy as np

from decs.schemas import DecsSchema
from decs.serialization import packb, unpackb


def test_numpy_packs_unpacks():
    np.random.seed(0)

    def check_array_packing_unpacking(data: np.ndarray):
        packed = packb(data)
        assert isinstance(packed, bytes)
        unpacked = unpackb(packed, model=np.ndarray[data.dtype])
        assert isinstance(unpacked, np.ndarray)
        assert np.all(unpacked == data)

    for dtype in (np.uint8, np.int32):
        data = np.random.randint(0, 256, size=(128, 128, 3), dtype=dtype)
        check_array_packing_unpacking(data)

    for dtype in (np.float32, np.float64):
        data = np.random.rand(128, 128, 3).astype(dtype)
        check_array_packing_unpacking(data)


def test_pydantic_packs_unpacks():
    class SimpleMessage(DecsSchema):
        message: str = Field(..., example="test")

    packed = packb(SimpleMessage(message="test"))
    assert isinstance(packed, bytes)

    unpacked = unpackb(packed, model=SimpleMessage)
    assert isinstance(unpacked, SimpleMessage)

    assert unpacked.message == "test"


def test_nested_pydantic_packs_unpacks():
    class SimpleMessage(DecsSchema):
        message: str

    class NestedMessage(DecsSchema):
        message: SimpleMessage
        step: int

    packed = packb(NestedMessage(message=SimpleMessage(message="test"), step=1))

    unpacked = unpackb(packed, model=NestedMessage)
    assert isinstance(unpacked, NestedMessage)
    assert isinstance(unpacked.message, SimpleMessage)
    assert unpacked.message.message == "test"

    assert unpacked.step == 1


def test_numpy_generics_in_pydantic_pack_unpack():
    np.random.seed(0)

    def check_packing_unpacking(data: np.ndarray, dtype: type):
        class Message(DecsSchema):
            message: str
            buffer: np.ndarray[dtype]

        packed = packb(Message(message="test", buffer=data))

        unpacked = unpackb(packed, model=Message)
        assert isinstance(unpacked, Message)
        assert unpacked.message == "test"
        assert np.all(unpacked.buffer == data)

    buffer = np.random.randint(0, 256, size=(128, 128, 3), dtype=np.uint8).astype(np.uint8)
    check_packing_unpacking(buffer, np.uint8)

    buffer = np.random.rand(128, 128, 3).astype(np.float32)
    check_packing_unpacking(buffer, np.float32)


def test_numpy_ndarray_in_pydantic_unpacks_as_uint8():
    np.random.seed(0)

    class Message(DecsSchema):
        message: str
        buffer: np.ndarray

    buffer = np.random.randint(0, 256, size=(128, 128, 3), dtype=np.uint8).astype(np.uint8)
    packed = packb(Message(message="test", buffer=buffer))

    unpacked = unpackb(packed, model=Message)
    assert isinstance(unpacked, Message)
    assert unpacked.message == "test"
    assert unpacked.buffer.dtype == np.uint8
    assert np.all(unpacked.buffer == buffer)

    buffer = np.random.randint(0, 256, size=(128, 128, 3), dtype=np.int32)
    packed = packb(Message(message="test", buffer=buffer))
    unpacked = unpackb(packed, model=Message)
    assert unpacked.buffer.dtype == np.uint8


def test_uuid_packs_unpacks():
    uid = uuid.uuid4()

    packed = packb(uid)
    unpacked = unpackb(packed, model=uuid.UUID)

    assert isinstance(unpacked, uuid.UUID)
    assert unpacked == uid


def test_uuid_in_pydantic_packs_unpacks():
    class Message(DecsSchema):
        uid: uuid.UUID

    uid = uuid.uuid4()

    packed = packb(Message(uid=uid))
    unpacked = unpackb(packed, model=Message)

    assert isinstance(unpacked, Message)
    assert isinstance(unpacked.uid, uuid.UUID)
    assert unpacked.uid == uid


def test_datetime_packs_unpacks():
    dt = datetime.now()

    packed = packb(dt)
    unpacked = unpackb(packed, model=datetime)

    assert isinstance(unpacked, datetime)
    assert unpacked == dt


def test_datetime_in_pydantic_packs_unpacks():
    class Message(DecsSchema):
        timestamp: datetime

    def check_packing_unpacking(timestamp: datetime):
        packed = packb(Message(timestamp=timestamp))
        unpacked = unpackb(packed, model=Message)

        assert isinstance(unpacked, Message)
        assert isinstance(unpacked.timestamp, datetime)
        assert unpacked.timestamp == timestamp
        if timestamp.tzinfo:
            assert unpacked.timestamp.tzinfo == timestamp.tzinfo

    check_packing_unpacking(datetime.now())
    check_packing_unpacking(datetime.now(timezone.utc))


# def test_png_image_packs_unpacks():
#     image_data = np.random.randint(0, 256, size=(128, 128, 3), dtype=np.uint8).astype(np.uint8)
#     image = PngImage(data=image_data)
#
#     packed = packb(image)
#     unpacked = unpackb(packed, model=PngImage)
#
#     assert isinstance(unpacked, PngImage)
#     assert np.all(unpacked.decode() == image_data)


def test_list_of_nested_pydantic_packs_unpacks():
    class SimpleMessage(DecsSchema):
        message: str

    class NestedMessage(DecsSchema):
        message: SimpleMessage
        step: int

    n_messages = 10

    messages = [NestedMessage(message=SimpleMessage(message="test"), step=idx) for idx in range(n_messages)]

    packed = packb(messages)

    unpacked = unpackb(packed, model=List[NestedMessage])
    assert isinstance(unpacked, list)
    for idx in range(n_messages):
        assert isinstance(unpacked[idx], NestedMessage)
        assert isinstance(unpacked[idx].message, SimpleMessage)
        assert unpacked[idx].message.message == "test"

        assert unpacked[idx].step == idx


def test_dict_with_nontrivial_types():
    class SimpleMessage(DecsSchema):
        message: str

    n_messages = 10

    messages = {str(idx): SimpleMessage(message=f"test{idx}") for idx in range(n_messages)}

    packed = packb(messages)

    unpacked = unpackb(packed, model=dict[str, SimpleMessage])
    assert isinstance(unpacked, dict)
    assert len(unpacked) == n_messages

    for idx in range(n_messages):
        key = str(idx)
        assert key in unpacked
        assert isinstance(unpacked[key], SimpleMessage)
        assert unpacked[key].message == f"test{idx}"
