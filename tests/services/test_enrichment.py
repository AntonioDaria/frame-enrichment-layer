"""Behaviour of the enrichment function: what colour a frame comes out carrying."""

from collections.abc import Callable

from app.schemas import ColorUpdate
from app.services import color_tracking
from app.services.enrichment import enrich
from app.services.ports import Message
from tests.conftest import FakeColorStore


def test_a_frame_for_a_room_with_a_known_colour_comes_out_carrying_that_colour(
    color_store: FakeColorStore, make_frame: Callable[..., Message]
) -> None:
    """A frame for a room with a known colour comes out carrying that colour."""
    color_store.set("room-1", (0.1, 1.0, 0.5))

    enriched = enrich(make_frame(room_id="room-1"), color_store)

    assert enriched["color"] == (0.1, 1.0, 0.5)


def test_a_frame_for_a_room_with_no_colour_yet_is_still_returned_with_a_null_colour(
    color_store: FakeColorStore, make_frame: Callable[..., Message]
) -> None:
    """A frame for a room with no colour yet is still returned with a null colour."""
    enriched = enrich(make_frame(room_id="room-never-seen"), color_store)

    assert enriched["color"] is None
    assert enriched["roomId"] == "room-never-seen"


def test_the_frame_the_caller_passed_in_is_left_untouched(
    color_store: FakeColorStore, make_frame: Callable[..., Message]
) -> None:
    """The frame the caller passed in is left untouched."""
    color_store.set("room-1", (0.1, 1.0, 0.5))
    frame = make_frame(room_id="room-1")
    before = dict(frame)

    enrich(frame, color_store)

    assert "color" not in frame
    assert frame == before


def test_the_frame_blob_is_carried_through_untouched_and_never_decoded(
    color_store: FakeColorStore, make_frame: Callable[..., Message]
) -> None:
    """The frame blob is carried through untouched and never decoded."""
    blob = "bm90LXZhbGlkLWJhc2U2NC0/Pz8="
    frame = make_frame(frame=blob)

    enriched = enrich(frame, color_store)

    # Identity, not equality: the blob is carried by reference, so nothing here
    # copies or decodes it. A blob that is not even valid base64 survives intact.
    assert enriched["frame"] is blob


def test_a_colour_change_between_two_frames_affects_only_the_later_frame(
    color_store: FakeColorStore, make_frame: Callable[..., Message]
) -> None:
    """The latest-colour-by-processing-time rule, which the whole design rests on."""
    color_tracking.apply(
        ColorUpdate.model_validate(
            {
                "roomId": "room-1",
                "newColor": [0.1, 1.0, 0.5],
                "timestamp": "2026-06-01T10:00:00",
            }
        ),
        color_store,
    )
    first = enrich(make_frame(room_id="room-1"), color_store)

    color_tracking.apply(
        ColorUpdate.model_validate(
            {
                "roomId": "room-1",
                "newColor": [0.9, 0.2, 0.3],
                "timestamp": "2026-06-01T10:15:00",
            }
        ),
        color_store,
    )
    second = enrich(make_frame(room_id="room-1"), color_store)

    assert second["color"] == (0.9, 0.2, 0.3)
    assert first["color"] == (0.1, 1.0, 0.5)


def test_frames_from_different_rooms_get_their_own_rooms_colours(
    color_store: FakeColorStore, make_frame: Callable[..., Message]
) -> None:
    """Frames from different rooms get their own room's colours."""
    color_store.set("room-1", (0.1, 1.0, 0.5))
    color_store.set("room-2", (0.9, 0.2, 0.3))

    assert enrich(make_frame(room_id="room-1"), color_store)["color"] == (0.1, 1.0, 0.5)
    assert enrich(make_frame(room_id="room-2"), color_store)["color"] == (0.9, 0.2, 0.3)
