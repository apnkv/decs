import asyncio
import logging
import math
import traceback
from typing import Awaitable, Callable, Type, TypeVar

import redis.asyncio as aioredis
from anyio import TASK_STATUS_IGNORED, create_memory_object_stream, create_task_group, to_thread

from decs.abc import ChannelLayer, DECS_PREFIX
from decs.asyncio.helper import AsyncHelper
from decs.events.abc import DeliveryMode, Event
from decs.events.sink import Sink
from decs.serialization import get_component_name, packb
from decs.queue_layer import get_default_channel_layer, get_default_redis_client, QueueLayer

logger = logging.getLogger(__name__)


class Publisher:
    _channel_layer: ChannelLayer
    _sinks: dict[Type[Event], Sink[Event]]
    _stop: bool

    def __init__(
        self,
        channel_layer: ChannelLayer = None,
        redis_client: aioredis.Redis = None,
        async_helper: AsyncHelper = None,
    ):
        self._channel_layer = channel_layer or get_default_channel_layer()
        self._redis_client = redis_client or get_default_redis_client()

        self._listeners = dict()
        self._sinks = dict()

        self._task_group = None

        self._serialize_sender, self._serialize_receiver = create_memory_object_stream(max_buffer_size=math.inf)
        self._publish_sender, self._publish_receiver = create_memory_object_stream(max_buffer_size=math.inf)

        self._helper = async_helper or AsyncHelper()
        self._helper.schedule(self._run)

    async def _run(self):
        async with create_task_group() as tg:
            self._task_group = tg
            await tg.start(self._serialize_events)
            await tg.start(self._publish_events)

        self._task_group = None

    async def _publish_events(self, task_status=TASK_STATUS_IGNORED):
        stop = False

        task_status.started()

        while not stop:
            try:
                logger.debug("Waiting for event to publish")
                packed_event, delivery_mode, component_name = await self._publish_receiver.receive()
                if packed_event is None:
                    stop = True

                if delivery_mode == DeliveryMode.ALL:
                    group_name = f"{DECS_PREFIX}:sinks:{component_name}:all"
                    await self._channel_layer.group_send(group_name, packed_event)

                elif delivery_mode == DeliveryMode.ANY:
                    channel_name = f"{DECS_PREFIX}:sinks:{component_name}:any"
                    await self._redis_client.rpush(channel_name, packed_event)

            except (asyncio.CancelledError, GeneratorExit):
                break

            except Exception:
                traceback.print_exc()

    async def _serialize_events(self, task_status=TASK_STATUS_IGNORED):
        stop = False

        task_status.started()

        while not stop:
            try:
                logger.debug("Waiting for event to serialize")
                event, delivery_mode = await self._serialize_receiver.receive()
                logger.debug(f"Received event: {event}")
                packed_event = await to_thread.run_sync(packb, event)
                logger.debug(f"Serialized event: {event}")
                await self._publish_sender.send((packed_event, delivery_mode, get_component_name(type(event))))

                if event is None:
                    stop = True

            except (asyncio.CancelledError, GeneratorExit):
                break

            except Exception:
                traceback.print_exc()

    async def _publish_event(self, event: Event, delivery_mode: DeliveryMode):
        await self._serialize_sender.send((event, delivery_mode))

    def publish(
        self,
        event: Event,
        mode: DeliveryMode = DeliveryMode.ALL,
    ):
        self._helper.schedule(self._publish_event, event, mode)

    def stop(self):
        self._serialize_sender.send_nowait((None, None))
        self._publish_sender.send_nowait((None, None, None))
        self._task_group.cancel_scope.cancel()
