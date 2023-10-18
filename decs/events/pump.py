import asyncio
import logging
import traceback
from collections import defaultdict
from functools import partial
from typing import Type

import anyio
from anyio import create_memory_object_stream, create_task_group, TASK_STATUS_IGNORED, to_thread

from decs.asyncio.helper import AsyncHelper
from decs.abc import ChannelLayer, DECS_PREFIX, Serializer, Deserializer
from decs.events.abc import DeliveryMode, Event
from decs.events.publisher import Publisher
from decs.events.sink import Sink
from decs.schemas import DecsSchema
from decs.serialization import get_component_name, packb, unpackb
from decs.queue_layer import get_default_channel_layer, get_default_redis_client, QueueLayer

logger = logging.getLogger(__name__)


class EventPump:
    _async_helper: AsyncHelper
    _queue_layer: QueueLayer
    _channel_layer: ChannelLayer

    _sinks: dict[str, list[Sink[Event]]]

    _event_type_to_queue_name: dict[Type[Event], str]
    _event_type_to_group_name: dict[Type[Event], str]

    def __init__(
        self,
        queue_layer: QueueLayer = None,
        channel_layer: ChannelLayer = None,
        async_helper: AsyncHelper = None,
        serializer: Serializer = packb,
        deserializer: Deserializer = unpackb,
    ):
        self._queue_layer = queue_layer or QueueLayer(get_default_redis_client())
        self._channel_layer = channel_layer or get_default_channel_layer()
        self._channel = None

        self._event_type_to_queue_name = dict()
        self._event_type_to_group_name = dict()
        self._event_type_to_group_channel = dict()

        self._task_group = create_task_group()

        self._sinks = defaultdict(list)

        self._serialize = serializer
        self._deserialize = deserializer

        self._async_helper = async_helper or AsyncHelper()
        self._async_helper.schedule(self._run)

    @staticmethod
    def _get_individual_channel_name(event_type: Type[Event]):
        return f"{DECS_PREFIX}:sinks:{get_component_name(event_type)}:any"

    def _get_group_name_and_channel(self, event_type: Type[Event]):
        group_name = f"{DECS_PREFIX}:sinks:{get_component_name(event_type)}:all"
        group_channel = self._channel_layer._get_group_channel_name(group_name)

        return group_name, group_channel

    async def _ensure_channel(self):
        if self._channel is None:
            self._channel = await self._channel_layer.new_channel()

    async def _ensure_group_channel(self, event_type: Type[Event]):
        if event_type in self._event_type_to_group_channel:
            return self._event_type_to_group_channel[event_type]

        await self._ensure_channel()

        group_name, group_channel = self._get_group_name_and_channel(event_type)
        await self._channel_layer.group_add(group_name, self._channel)

        self._event_type_to_group_channel[event_type] = group_channel
        return group_channel

    async def handle_raw_event(self, event: Event, event_type: Type[Event]):
        event = await to_thread.run_sync(self._deserialize, event, event_type)
        component_name = get_component_name(event_type)
        for sink in self._sinks[component_name]:
            sink.feed(event)

    async def _group_listener_loop(self, event_type: Type[Event], task_status=TASK_STATUS_IGNORED):
        group_channel = await self._ensure_group_channel(event_type)
        task_status.started()

        try:
            while True:
                try:
                    event = await self._receive_from_channel(group_channel)
                    logger.debug(f"Received event: {event}")
                    if event is None:
                        break
                    self._task_group.start_soon(self.handle_raw_event, event, event_type)

                except (asyncio.CancelledError, asyncio.TimeoutError, GeneratorExit):
                    break

                except Exception:
                    traceback.print_exc()
        finally:
            group_name = self._event_type_to_group_name[event_type]
            await self._channel_layer.group_discard(group_name, self._channel)

    async def _individual_listener_loop(self, event_type: Type[Event], task_status=TASK_STATUS_IGNORED):
        if event_type not in self._event_type_to_queue_name:
            self._event_type_to_queue_name[event_type] = self._get_individual_channel_name(event_type)

        queue_name = self._event_type_to_queue_name[event_type]

        task_status.started()

        while True:
            try:
                event = await self._queue_layer.blpop(queue_name, timeout=0)
                logger.debug(f"Received event: {event}")
                self._task_group.start_soon(self.handle_raw_event, event, event_type)

            except (asyncio.CancelledError, asyncio.TimeoutError, GeneratorExit):
                break

            except Exception:
                traceback.print_exc()

    async def _ensure_listeners_for_event_type(self, event_type: Type[Event], task_status=TASK_STATUS_IGNORED):
        if event_type in self._event_type_to_group_name and event_type in self._event_type_to_queue_name:
            return

        group_name, group_channel = self._get_group_name_and_channel(event_type)
        queue_name = self._get_individual_channel_name(event_type)

        self._event_type_to_group_name[event_type] = group_name
        self._event_type_to_queue_name[event_type] = queue_name

        await self._task_group.start(self._group_listener_loop, event_type)
        await self._task_group.start(self._individual_listener_loop, event_type)

        task_status.started()

    def add_handler(self, event_type: Type[Event], handler):
        component_name = get_component_name(event_type)
        if not self._sinks[component_name]:
            self.add_sink(Sink(event_type))

        self._sinks[component_name][-1].add_handler(handler)

    def add_sink(self, sink: Sink[Event]):
        component_name = get_component_name(sink._event_type)
        self._sinks[component_name].append(sink)

        # TODO: this _might_ require a task group!
        self._async_helper.run(self._ensure_listeners_for_event_type, sink._event_type)

    async def _receive_from_channel(self, channel_name: str):
        try:
            logger.debug(f"Waiting for message on channel {channel_name}")
            return await self._channel_layer.receive(channel_name)
        except (asyncio.CancelledError, asyncio.TimeoutError, GeneratorExit):
            traceback.print_exc()
            return None

    async def _sleep_until_stopped(self):
        while True:
            await asyncio.sleep(100)

    async def _run(self):
        async with self._task_group as tg:
            await self._sleep_until_stopped()

        self._task_group = None


class TestEvent(DecsSchema):
    name: str
    age: int


async def async_handler(event: TestEvent):
    logger.debug(f"async_handler: {event}")


def sync_handler(event: TestEvent):
    logger.debug(f"sync_handler: {event}")


async def main():
    logging.basicConfig(level=logging.DEBUG)

    pump = EventPump(
        QueueLayer(get_default_redis_client()),
        get_default_channel_layer(),
    )

    await asyncio.sleep(0.5)

    sink = Sink(TestEvent)
    sink.add_handler(async_handler)
    sink.add_handler(sync_handler)
    pump.add_sink(sink)

    await asyncio.sleep(0.5)

    dispatcher = Publisher()
    dispatcher.publish(TestEvent(name="test", age=10), DeliveryMode.ANY)

    await asyncio.sleep(10)

    dispatcher.stop()


if __name__ == "__main__":
    anyio.run(main)
