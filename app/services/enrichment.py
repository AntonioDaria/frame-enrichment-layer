"""The heart of the enrichment layer: attach a room's current colour to a frame."""

from app.services.ports import ColorStore, Message


def enrich(frame: Message, color_store: ColorStore) -> Message:
    """Return the frame with its room's current colour attached.

    This is the one piece of logic the whole service exists for, so it is kept
    pure: given a parsed frame and anything that can answer get(room_id), it
    returns the enriched frame. It imports no broker and no cache, which is what
    makes it testable with no infrastructure at all.

    Frames stay raw dicts with the wire's camelCase keys rather than becoming
    pydantic models. At 500 frames/s carrying 100-500 KB blobs, the unavoidable
    per-frame cost is one JSON parse in and one JSON serialize out; a model
    round-trip would add a third and fourth pass over the same data for no gain.
    The base64 blob is never decoded and never inspected: it is carried by
    reference into the new dict, so nothing here scales with frame size.

    The output is a new dict rather than a mutated input. The caller may still
    hold the original (a redelivery, a test, a future retry), and silently
    changing it under them is the kind of bug that only shows up under load.

    A room with no colour yet yields None, and the frame is still returned. A
    frame with no colour is more useful to the Inference Service than no frame,
    and it self-corrects on the room's first colour update. That case is normal
    cold start rather than an error, and it is deliberately not logged here:
    this function runs 500 times a second, so per-frame logging would be 500
    lines a second. The worker warns once per room instead.

    The frame is assumed to carry roomId already; the worker's poison-message
    guard rejects malformed frames before they reach this function.
    """
    return {**frame, "color": color_store.get(frame["roomId"])}
