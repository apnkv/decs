import asyncio
import logging
import pytest
import uuid

from decs.events.pump import EventPump
from decs.events.publisher import Publisher
from decs.schemas import DecsSchema

logger = logging.getLogger(__name__)


class InferenceStarted(DecsSchema):
    uid: uuid.UUID


class InferenceProgressUpdate(DecsSchema):
    uid: uuid.UUID

    step: int
    total_steps: int | None = None


class InferenceImageUpdate(DecsSchema):
    uid: uuid.UUID

    image_uid: uuid.UUID
    image: bytes


class InferenceFinished(DecsSchema):
    uid: uuid.UUID


class InferenceProcess:
    def __init__(self, uid: uuid.UUID):
        self._step = 0
        self._total_steps = 0
        self._uid = uid

    def set_step(self, step: int):
        self._step = step

    def set_total_steps(self, total_steps: int):
        self._total_steps = total_steps

    @property
    def step(self):
        return self._step

    @property
    def total_steps(self):
        return self._total_steps


class InferenceSystem:
    _inferences: dict[uuid.UUID] = dict()

    @staticmethod
    async def handle_start(event: InferenceStarted):
        InferenceSystem._inferences[event.uid] = InferenceProcess(event.uid)

        logger.info(f"Started generation: {event.uid}")

    @staticmethod
    async def handle_progress_update(event: InferenceProgressUpdate):
        process = InferenceSystem._inferences[event.uid]
        process._step = event.step
        process._total_steps = event.total_steps

        logger.info(f"Progress: {process.step}/{process.total_steps}")

    @staticmethod
    async def handle_finished(event: InferenceImageUpdate):
        logger.info(f"Finished generation: {event.uid}")

        if event.uid in InferenceSystem._inferences:
            del InferenceSystem._inferences[event.uid]


class TestMockInference:
    @pytest.mark.asyncio
    async def test_mock_inference(self):
        logging.basicConfig(level=logging.DEBUG)

        pump = EventPump()
        pump.add_handler(InferenceStarted, InferenceSystem.handle_start)
        pump.add_handler(InferenceProgressUpdate, InferenceSystem.handle_progress_update)
        pump.add_handler(InferenceFinished, InferenceSystem.handle_finished)

        await asyncio.sleep(0.3)

        publisher = Publisher()
        await asyncio.sleep(0.02)

        uid1 = uuid.uuid4()
        uid2 = uuid.uuid4()

        publisher.publish(InferenceStarted(uid=uid2))
        publisher.publish(InferenceStarted(uid=uid1))

        for step in range(10):
            publisher.publish(InferenceProgressUpdate(uid=uid1, step=step + 1, total_steps=10))
            publisher.publish(InferenceProgressUpdate(uid=uid2, step=2 * (step + 1), total_steps=20))
            await asyncio.sleep(0.05)

        assert len(InferenceSystem._inferences) == 2
        assert InferenceSystem._inferences[uid1].step == 10
        assert InferenceSystem._inferences[uid1].total_steps == 10
        assert InferenceSystem._inferences[uid2].step == 20
        assert InferenceSystem._inferences[uid2].total_steps == 20

        publisher.publish(InferenceFinished(uid=uid1))
        await asyncio.sleep(0.02)

        assert len(InferenceSystem._inferences) == 1

        publisher.publish(InferenceFinished(uid=uid2))
        await asyncio.sleep(0.02)

        assert len(InferenceSystem._inferences) == 0
