"""Message shapes at the system boundaries, kept byte-compatible with the brief."""

from pydantic import BaseModel, ConfigDict, Field


class FrameIn(BaseModel):
    """A camera frame as it arrives on the frames topic.

    This model exists to document and to test the inbound contract, not to run on
    the frame hot path. See EnrichedFrame for why the hot path stays dict-based.

    The frame body is an opaque str. The enrichment layer never decodes it and
    never validates it as base64: it only ever carries the blob from one topic to
    the next, so decoding would buy nothing and cost real work at 500 frames/s.

    The timestamp is also carried as an opaque str. The layer neither reads nor
    reasons about it, and parsing it into a datetime would risk re-serialising it
    into a different-looking value (offset normalisation, microseconds). The brief
    requires the JSON schemas to be preserved at the boundaries, so passing the
    value through untouched is the only way to guarantee that.
    """

    model_config = ConfigDict(populate_by_name=True)

    room_id: str = Field(alias="roomId")
    camera_id: str = Field(alias="cameraId")
    frame: str
    timestamp: str


class ColorUpdate(BaseModel):
    """A light colour change as it arrives on the colours topic.

    Colour updates are the cold path (about 0.1 messages/s for the whole system),
    so unlike frames they are worth validating properly: the cost is irrelevant
    and a malformed colour would otherwise poison every frame in that room.

    Validation is deliberately shape-only. A colour must be three floats, but no
    range constraint is applied, because a component outside the usual 0.0-1.0 is
    still real data from a real light. Rejecting it here would silently drop the
    room's colour and leave every subsequent frame stale; the colours path logs
    the oddity instead and stores the value.
    """

    model_config = ConfigDict(populate_by_name=True)

    room_id: str = Field(alias="roomId")
    new_color: tuple[float, float, float] = Field(alias="newColor")
    timestamp: str


class EnrichedFrame(FrameIn):
    """A frame as the Inference Service consumes it: the input plus one field.

    Subclassing FrameIn is the point of this model. The Inference Service is a
    black box expecting the inbound fields back unchanged, so restating them here
    would let the two definitions drift apart; inheriting them makes that
    impossible.

    Like FrameIn, this is executable documentation and a test fixture. It is
    deliberately NOT constructed on the frame hot path. That path is dict in,
    dict out: one JSON parse on the way in (needed to reach roomId, which sits in
    the same object as the blob), one added "color" key, one JSON serialize on
    the way out. No base64 decode and no pydantic round-trip, because those two
    JSON operations are the only unavoidable per-frame cost and everything else
    is avoidable.

    color is None until the room's first colour update arrives. A frame with no
    colour is more useful to the Inference Service than no frame at all, and it
    self-corrects on that first update.
    """

    color: tuple[float, float, float] | None
