"""Behaviour of colour tracking: which colour a room ends up with, and when."""

import structlog

from app.schemas import ColorUpdate
from app.services import color_tracking
from tests.conftest import FakeColorStore


def _update(
    room_id: str = "room-1",
    new_color: list[float] | None = None,
    timestamp: str = "2026-06-01T10:00:00",
) -> ColorUpdate:
    """Build a wire-shaped colour update, so each test states only what it varies."""
    return ColorUpdate.model_validate(
        {
            "roomId": room_id,
            "newColor": new_color if new_color is not None else [0.1, 1.0, 0.5],
            "timestamp": timestamp,
        }
    )


def test_applying_an_update_makes_the_store_return_that_colour_for_the_room(
    color_store: FakeColorStore,
) -> None:
    """Applying an update makes the store return that colour for the room."""
    update = _update(room_id="room-1", new_color=[0.1, 1.0, 0.5])

    color_tracking.apply(update, color_store)

    assert color_store.get("room-1") == (0.1, 1.0, 0.5)


def test_a_later_update_wins_even_when_its_own_timestamp_is_older(
    color_store: FakeColorStore,
) -> None:
    """Arrival order decides, because the rule is latest by processing time."""
    color_tracking.apply(
        _update(new_color=[0.1, 1.0, 0.5], timestamp="2026-06-01T10:15:00"), color_store
    )
    color_tracking.apply(
        _update(new_color=[0.9, 0.2, 0.3], timestamp="2026-06-01T10:00:00"), color_store
    )

    assert color_store.get("room-1") == (0.9, 0.2, 0.3)


def test_an_update_for_one_room_leaves_other_rooms_alone(
    color_store: FakeColorStore,
) -> None:
    color_tracking.apply(
        _update(room_id="room-1", new_color=[0.1, 1.0, 0.5]), color_store
    )
    color_tracking.apply(
        _update(room_id="room-2", new_color=[0.9, 0.2, 0.3]), color_store
    )

    assert color_store.get("room-1") == (0.1, 1.0, 0.5)


def test_a_colour_component_outside_the_usual_range_is_stored_and_logged(
    color_store: FakeColorStore,
) -> None:
    """A stray reading is real data; dropping it would stale every later frame."""
    with structlog.testing.capture_logs() as logs:
        color_tracking.apply(_update(new_color=[-0.2, 1.7, 0.5]), color_store)

    assert color_store.get("room-1") == (-0.2, 1.7, 0.5)
    assert [entry["log_level"] for entry in logs] == ["warning"]


def test_an_in_range_colour_is_stored_without_a_warning(
    color_store: FakeColorStore,
) -> None:
    with structlog.testing.capture_logs() as logs:
        color_tracking.apply(_update(new_color=[0.0, 0.5, 1.0]), color_store)

    assert color_store.get("room-1") == (0.0, 0.5, 1.0)
    assert logs == []
