import asyncio
import logging
import traceback

import anyio
import threading
from anyio import create_task_group
from concurrent.futures import Future
from functools import partial

from anyio.from_thread import BlockingPortal
from typing import Callable, Coroutine, TypeVar, ParamSpec


T = TypeVar("T")
P = ParamSpec("P")
CoroutineCallable = Callable[P, Coroutine[T, None, None]]

logger = logging.getLogger(__name__)


class AsyncHelper:
    _instance: "AsyncHelper | None" = None
    _portal: BlockingPortal | None

    _started: bool
    _stop: bool
    _start_cv: threading.Condition

    _pending_tasks: set[Future]

    def __init__(self):
        self._thread = threading.Thread(
            target=anyio.run,
            args=(self._portal_main,),
            daemon=True,
        )
        self._portal = None

        self._started = False
        self._stop = False
        self._restart = False
        self._start_cv = threading.Condition()
        self._exception = None

        self._thread.start()
        with self._start_cv:
            self._start_cv.wait_for(lambda: self._started)

        self._pending_tasks = set()

        AsyncHelper._instance = self

    async def _portal_main(self):
        while not self._stop:
            self._portal = BlockingPortal()
            self._restart = False

            async with self._portal:
                logger.debug("Starting portal")
                self._started = True
                with self._start_cv:
                    self._start_cv.notify_all()

                await self._portal.sleep_until_stopped()

            if self._exception is not None and not self._restart:
                raise self._exception

    @staticmethod
    def instance():
        if AsyncHelper._instance is None:
            AsyncHelper._instance = AsyncHelper()

        return AsyncHelper._instance

    def run(self, fn: CoroutineCallable, *args: P.args, **kwargs: P.kwargs) -> T:
        return self._portal.call(partial(fn, **kwargs), *args)

    def handle_exceptions(self, future: Future) -> None:
        try:
            future.result()

        except Exception:
            traceback.print_exc()

            self._restart = True
            asyncio.create_task(self._kill_portal())

        finally:
            self._pending_tasks.remove(future)

    def schedule(self, fn: CoroutineCallable, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        future = self._portal.start_task_soon(partial(fn, **kwargs), *args)

        future.add_done_callback(self.handle_exceptions)
        self._pending_tasks.add(future)

        return future

    async def _kill_portal(self):
        if self._portal is not None:
            await self._portal.stop()
            self._portal = None

    def stop(self):
        self._stop = True
        if self._portal is not None:
            self.run(self._kill_portal)
        self._thread.join()

    def __del__(self):
        self.stop()
