import threading
import time

from decs.channels import MessageChannel, MessageDispatcher
from decs.schemas import DurerSchema, JpegImage


class SimpleMessage(DurerSchema):
    message: str | None


def test_message_channel_works():
    channel = MessageDispatcher.instance().get_channel("test", "test", force_open=True)
    assert isinstance(channel, MessageChannel)

    channel.push(SimpleMessage(message="test"))
    assert channel.pop(SimpleMessage).message == "test"


def test_message_channel_delivers_messages_after_closing():
    entity_id = "test"
    suffix = "test"
    n_messages = 5

    class Message(DurerSchema):
        number: int

    def producer_thread():
        channel = MessageDispatcher.instance().get_channel(entity_id, suffix)
        try:
            for step in range(n_messages):
                time.sleep(0.01)
                channel.put(Message(number=step))
        finally:
            channel.close_after_pushing_pending_messages()

    receiver_side_channel = MessageDispatcher.instance().get_channel(entity_id, suffix, force_open=True)
    assert isinstance(receiver_side_channel, MessageChannel)

    thread = threading.Thread(target=producer_thread)
    thread.start()

    received_messages = []
    while True:
        try:
            message = receiver_side_channel.pop(Message)
            assert message is not None
            received_messages.append(message)
        except receiver_side_channel.Closed:
            break

    thread.join()

    assert len(received_messages) == n_messages
    assert all(message.number == i for i, message in enumerate(received_messages))

