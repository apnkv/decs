import asyncio
import inspect
import logging
import traceback
from typing import (
    Awaitable,
    Callable,
    Generic,
    Type,
)

import redis.asyncio as aioredis
from anyio import create_memory_object_stream, create_task_group, to_thread

from decs.abc import ChannelLayer, Serializer, Deserializer
from decs.asyncio.helper import AsyncHelper
from decs.events.abc import Event
from decs.serialization import packb, unpackb
from decs.queue_layer import get_default_channel_layer, get_default_redis_client


logger = logging.getLogger(__name__)


def get_async_helper():
    if not hasattr(get_async_helper, "_instance"):
        get_async_helper._instance = AsyncHelper()

    return get_async_helper._instance


class Sink(Generic[Event]):
    _handlers: set[Callable[[Event], Awaitable[None]]]

    def __init__(
        self,
        event_type: Type[Event],
        channel_layer: ChannelLayer = None,
        redis_client: aioredis.Redis = None,
        serialize: Serializer = packb,
        deserialize: Deserializer = unpackb,
    ):
        self._channel_layer = channel_layer or get_default_channel_layer()
        self._redis_client = redis_client or get_default_redis_client()
        self._channel = None

        self._event_type = event_type
        self._in_group = False

        self._serialize = serialize
        self._deserialize = deserialize

        self._handlers = set()
        self._pending_handlers = set()

        self._task_group = None
        self._send_to_dispatch, self._receive_to_dispatch = create_memory_object_stream()

        helper = get_async_helper()
        helper.schedule(self._run)

    async def _run(self):
        try:
            async with create_task_group() as tg:
                self._task_group = tg
                tg.start_soon(self._dispatch_loop)
        # except* (asyncio.CancelledError, asyncio.TimeoutError, GeneratorExit):
        #     pass
        except* Exception as excgroup:
            for exc in excgroup.exceptions:
                raise exc

        self._task_group = None

    def add_handler(self, handler: Callable[[Event], Awaitable[None]] | Callable[[Event], None]):
        self._handlers.add(handler)

    def remove_handler(self, handler: Callable[[Event], Awaitable[None]] | Callable[[Event], None]):
        self._handlers.remove(handler)

    async def _handle_execution_result(
        self,
        handler: Callable[[Event], Awaitable[None]],
        event: Event,
    ):
        coro = None
        try:
            if not inspect.iscoroutinefunction(handler):
                coro = to_thread.run_sync(handler, event)
            else:
                coro = handler(event)
            self._pending_handlers.add(coro)
            await coro
        except Exception:
            traceback.print_exc()
        finally:
            if coro is not None:
                self._pending_handlers.remove(coro)

    def feed(self, event: Event):
        get_async_helper().schedule(self._send_to_dispatch.send, event)

    async def _dispatch_loop(self):
        while True:
            try:
                event: Event = await self._receive_to_dispatch.receive()
                logger.debug(f"Dispatching event: {event}")
                for handler in self._handlers:
                    self._task_group.start_soon(self._handle_execution_result, handler, event)

            except (asyncio.CancelledError, asyncio.TimeoutError, GeneratorExit):
                break

            except Exception:
                traceback.print_exc()

    def get_channel(self):
        return self._channel
