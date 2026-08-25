"""Behaviour of the replaceable edges: the colour store and the pub/sub stub."""

import asyncio
from collections.abc import Callable

import pytest

from app.adapters.color_store import InMemoryColorStore
from app.adapters.pubsub import InMemoryPubSub
from app.services.ports import Message


def test_a_room_with_no_colour_yet_reads_back_as_none() -> None:
    """Cold start is a normal answer from the store, not an error."""
    store = InMemoryColorStore()

    assert store.get("room-1") is None


def test_a_stored_colour_reads_back_and_a_later_one_replaces_it() -> None:
    """Only the latest colour is ever needed, so writing overwrites."""
    store = InMemoryColorStore()

    store.set("room-1", (0.1, 1.0, 0.5))
    assert store.get("room-1") == (0.1, 1.0, 0.5)

    store.set("room-1", (0.9, 0.2, 0.3))
    assert store.get("room-1") == (0.9, 0.2, 0.3)


def test_each_keys_messages_arrive_in_publish_order_however_they_interleave(
    make_frame: Callable[..., Message],
) -> None:
    """Per-key ordering is the guarantee the no-reordering rule depends on."""
    broker = InMemoryPubSub()
    received: list[tuple[str, str]] = []
    broker.subscribe(
        "frames",
        lambda value: received.append((value["cameraId"], value["timestamp"])),
    )

    interleaved = [
        ("camera-a", "10:00:01"),
        ("camera-b", "10:00:01"),
        ("camera-a", "10:00:02"),
        ("camera-a", "10:00:03"),
        ("camera-b", "10:00:02"),
    ]
    for camera_id, timestamp in interleaved:
        broker.publish(
            "frames",
            key=camera_id,
            value=make_frame(camera_id=camera_id, timestamp=timestamp),
        )

    broker.deliver_pending()

    for camera_id in ("camera-a", "camera-b"):
        published = [stamp for camera, stamp in interleaved if camera == camera_id]
        delivered = [stamp for camera, stamp in received if camera == camera_id]
        assert delivered == published


def test_a_message_whose_handler_raises_is_redelivered_rather_than_lost() -> None:
    """At-least-once: nothing is acknowledged until its handler has returned."""
    broker = InMemoryPubSub()
    attempts: list[str] = []
    handled: list[str] = []

    def handler(value: Message) -> None:
        attempts.append(value["id"])
        if len(attempts) == 1:
            raise RuntimeError("transient failure on first sight")
        handled.append(value["id"])

    broker.subscribe("frames", handler)
    broker.publish("frames", key="camera-a", value={"id": "frame-1"})

    acknowledged = broker.deliver_pending()

    assert attempts == ["frame-1", "frame-1"]
    assert handled == ["frame-1"]
    assert acknowledged == 1


def test_a_key_that_is_failing_never_lets_its_next_message_overtake() -> None:
    """Head-of-line blocking per key is what keeps a camera's frames in order."""
    broker = InMemoryPubSub()
    seen: list[str] = []

    def handler(value: Message) -> None:
        seen.append(value["id"])
        if value["id"] == "frame-1" and seen.count("frame-1") < 3:
            raise RuntimeError("still failing")

    broker.subscribe("frames", handler)
    broker.publish("frames", key="camera-a", value={"id": "frame-1"})
    broker.publish("frames", key="camera-a", value={"id": "frame-2"})

    broker.deliver_pending()

    assert seen.index("frame-2") > seen.index("frame-1")
    assert seen[: seen.index("frame-2")] == ["frame-1", "frame-1", "frame-1"]


def test_a_handler_that_always_raises_stops_instead_of_spinning_forever() -> None:
    """The stub has to terminate; a poison message stays pending, not looping."""
    broker = InMemoryPubSub()
    attempts: list[str] = []

    def handler(value: Message) -> None:
        attempts.append(value["id"])
        raise RuntimeError("permanent failure")

    broker.subscribe("frames", handler)
    broker.publish("frames", key="camera-a", value={"id": "frame-1"})

    acknowledged = broker.deliver_pending(max_attempts=3)

    assert attempts == ["frame-1"] * 3
    assert acknowledged == 0


def test_messages_on_a_topic_nobody_subscribed_to_simply_wait() -> None:
    """The worker publishes enriched frames to a topic it does not consume."""
    broker = InMemoryPubSub()
    broker.publish("enriched-frames", key="camera-a", value={"id": "frame-1"})

    assert broker.deliver_pending() == 0

    received: list[Message] = []
    broker.subscribe("enriched-frames", received.append)

    assert broker.deliver_pending() == 1
    assert received == [{"id": "frame-1"}]


@pytest.mark.parametrize("call", ["apublish", "asubscribe"])
def test_the_async_client_methods_are_declared_but_not_implemented(call: str) -> None:
    """Rule 4 asks for async placeholders; only the sync path is exercised."""
    broker = InMemoryPubSub()
    coroutines = {
        "apublish": lambda: broker.apublish("frames", key="camera-a", value={}),
        "asubscribe": lambda: broker.asubscribe("frames", _unused_async_handler),
    }

    with pytest.raises(NotImplementedError):
        asyncio.run(coroutines[call]())


async def _unused_async_handler(value: Message) -> None:
    """Stand in for an async consumer callback; never actually invoked."""
