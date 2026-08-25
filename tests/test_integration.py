"""The whole path end to end: real broker stub, real store, real worker."""

import time
from collections.abc import Callable

import structlog

from app.adapters.color_store import InMemoryColorStore
from app.adapters.pubsub import InMemoryPubSub
from app.config import Config
from app.services.ports import Message
from app.worker import EnrichmentWorker


def _colour_update(
    room_id: str = "room-1",
    new_color: list[float] | None = None,
    timestamp: str = "2026-06-01T10:00:05",
) -> Message:
    """Build a wire-shaped colour update, so each test states only what it varies."""
    return {
        "roomId": room_id,
        "newColor": new_color if new_color is not None else [0.1, 1.0, 0.5],
        "timestamp": timestamp,
    }


def _wire_up() -> tuple[InMemoryPubSub, Config, list[Message]]:
    """Assemble the real service and a collector on its output topic.

    Nothing here is faked. The tests drive the same broker stub, colour store and
    worker that `python -m app.worker` runs, which is the point of an integration
    test: it proves the wiring, not the pieces.

    The worker is constructed but not returned. Constructing it is what registers
    its handlers, and the broker holds those bound methods, so the worker stays
    alive without a reference here. Delivery is driven through the broker because
    draining is the stub's concern: a real broker pushes to its consumers, and
    the worker has no say in when that happens.
    """
    config = Config.from_env()
    broker = InMemoryPubSub()
    EnrichmentWorker(broker=broker, color_store=InMemoryColorStore(), config=config)
    enriched: list[Message] = []
    broker.subscribe(config.enriched_topic, enriched.append)
    return broker, config, enriched


def test_a_frame_before_any_colour_gets_null_and_a_later_frame_gets_the_colour(
    make_frame: Callable[..., Message],
) -> None:
    """Latest by processing time, proven through the real broker and worker."""
    broker, config, enriched = _wire_up()

    broker.publish(
        config.frames_topic, key="camera-a", value=make_frame(timestamp="10:00:00")
    )
    broker.deliver_pending()

    broker.publish(config.colours_topic, key="room-1", value=_colour_update())
    broker.deliver_pending()

    broker.publish(
        config.frames_topic, key="camera-a", value=make_frame(timestamp="10:00:10")
    )
    broker.deliver_pending()

    assert [frame["color"] for frame in enriched] == [None, (0.1, 1.0, 0.5)]


def test_the_enriched_frame_is_the_inbound_frame_plus_exactly_one_field(
    make_frame: Callable[..., Message],
) -> None:
    """The Inference Service is a black box, so the boundary schema must survive."""
    broker, config, enriched = _wire_up()
    inbound = make_frame()

    broker.publish(config.frames_topic, key="camera-a", value=inbound)
    broker.deliver_pending()

    assert enriched[0] == {**inbound, "color": None}
    assert set(enriched[0]) == {"roomId", "cameraId", "frame", "timestamp", "color"}
    assert enriched[0]["frame"] is inbound["frame"]


def test_each_cameras_frames_stay_in_order_across_the_whole_path(
    make_frame: Callable[..., Message],
) -> None:
    """The one ordering rule the brief sets, checked end to end rather than per hop."""
    broker, config, enriched = _wire_up()
    published = [
        ("camera-a", "10:00:01"),
        ("camera-b", "10:00:01"),
        ("camera-a", "10:00:02"),
        ("camera-a", "10:00:03"),
        ("camera-b", "10:00:02"),
        ("camera-b", "10:00:03"),
    ]

    for camera_id, timestamp in published:
        broker.publish(
            config.frames_topic,
            key=camera_id,
            value=make_frame(camera_id=camera_id, timestamp=timestamp),
        )
    broker.deliver_pending()

    for camera_id in ("camera-a", "camera-b"):
        expected = [stamp for camera, stamp in published if camera == camera_id]
        actual = [
            frame["timestamp"]
            for frame in enriched
            if frame["cameraId"] == camera_id
        ]
        assert actual == expected


def test_a_malformed_frame_is_dropped_and_the_frames_around_it_still_arrive(
    make_frame: Callable[..., Message],
) -> None:
    """A poison message must not take its partition down with it."""
    broker, config, enriched = _wire_up()

    broker.publish(
        config.frames_topic, key="camera-a", value=make_frame(timestamp="10:00:01")
    )
    broker.publish(
        config.frames_topic,
        key="camera-a",
        value={"cameraId": "camera-a", "frame": "abc", "timestamp": "10:00:02"},
    )
    broker.publish(
        config.frames_topic, key="camera-a", value=make_frame(timestamp="10:00:03")
    )

    with structlog.testing.capture_logs() as logs:
        broker.deliver_pending()

    assert [frame["timestamp"] for frame in enriched] == ["10:00:01", "10:00:03"]
    assert any(entry["log_level"] == "error" for entry in logs)


def test_a_malformed_colour_update_is_dropped_and_leaves_the_colour_unchanged(
    make_frame: Callable[..., Message],
) -> None:
    """A bad colour would otherwise be attached to every later frame in the room."""
    broker, config, enriched = _wire_up()

    broker.publish(config.colours_topic, key="room-1", value=_colour_update())
    broker.deliver_pending()

    broker.publish(
        config.colours_topic,
        key="room-1",
        value={"roomId": "room-1", "newColor": "not-a-colour", "timestamp": "10:01"},
    )
    with structlog.testing.capture_logs() as logs:
        broker.deliver_pending()

    broker.publish(config.frames_topic, key="camera-a", value=make_frame())
    broker.deliver_pending()

    assert enriched[0]["color"] == (0.1, 1.0, 0.5)
    assert any(entry["log_level"] == "error" for entry in logs)


def test_the_per_message_processing_overhead_is_far_below_the_required_rate(
    make_frame: Callable[..., Message],
) -> None:
    """Evidence for the 500 frames/s requirement, on the dimension it can cover.

    This measures per-message PROCESSING OVERHEAD only: the parse-free dict work
    of a lookup, a shallow copy and a publish. It says nothing about bandwidth,
    which is the actual constraint at 50-250 MB/s and is a property of the
    network and the deployment, not of this process. That argument belongs in the
    README, not in an assertion.

    The frames here carry a short placeholder blob rather than a real 100-500 KB
    one, precisely because size is the dimension this test does not speak to.

    The budget is deliberately loose. The point is the order of magnitude, not a
    benchmark, and a test that fails on a loaded CI runner would be worse than no
    test at all. Note too that 500 frames/s is the aggregate across all workers
    and partitions, so a single worker only has to show ample headroom.
    """
    broker, config, enriched = _wire_up()
    frame_count = 2000

    for index in range(frame_count):
        broker.publish(
            config.frames_topic,
            key=f"camera-{index % 5}",
            value=make_frame(camera_id=f"camera-{index % 5}", frame="c21hbGw="),
        )

    started = time.perf_counter()
    broker.deliver_pending()
    elapsed = time.perf_counter() - started

    assert len(enriched) == frame_count
    assert elapsed < 2.0, f"{frame_count} frames took {elapsed:.2f}s"
