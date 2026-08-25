"""Shared test doubles and builders: real collaborators, not mocks."""

from collections.abc import Callable

import pytest

from app.services.ports import Color, Message


class FakeColorStore:
    """A ColorStore over a plain dict, used instead of a mock.

    The tests are about the latest-colour rule, which is a question of what the
    store actually returns after a sequence of writes. A mock would only record
    that get() was called; this returns real stored values, so a test can assert
    the behaviour rather than the interaction.
    """

    def __init__(self) -> None:
        self._colors: dict[str, Color] = {}

    def get(self, room_id: str) -> Color | None:
        """Return the room's current colour, or None if it has none yet."""
        return self._colors.get(room_id)

    def set(self, room_id: str, color: Color) -> None:
        """Record a room's new colour, replacing any previous one."""
        self._colors[room_id] = color


@pytest.fixture
def color_store() -> FakeColorStore:
    """Give each test its own empty store, so cold start is the default state."""
    return FakeColorStore()


@pytest.fixture
def make_frame() -> Callable[..., Message]:
    """Build wire-shaped frame dicts so each test states only what it varies.

    Frames are dicts with the wire's camelCase keys, matching what the worker
    hands to enrich() after one JSON parse.
    """

    def _make_frame(
        room_id: str = "room-1",
        camera_id: str = "camera-a",
        frame: str = "ZnJhbWUtYnl0ZXM=",
        timestamp: str = "2026-06-01T10:00:00",
    ) -> Message:
        return {
            "roomId": room_id,
            "cameraId": camera_id,
            "frame": frame,
            "timestamp": timestamp,
        }

    return _make_frame
