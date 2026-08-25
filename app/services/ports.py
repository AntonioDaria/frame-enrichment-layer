"""The Protocols the core logic depends on, so it never imports a cache or broker."""

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

type Color = tuple[float, float, float]
"""An RGB light colour as the brief sends it: three floats, no range constraint."""

type Message = dict[str, Any]
"""A decoded message body.

Deliberately a plain dict rather than a pydantic model: the frame path is dict
in, dict out, and a 100-500 KB blob should be parsed once and serialised once.
"""

type MessageHandler = Callable[[Message], None]
"""A sync consumer callback. Returning normally acks; raising redelivers."""

type AsyncMessageHandler = Callable[[Message], Awaitable[None]]
"""The async equivalent of MessageHandler, with the same ack contract."""


class ColorStore(Protocol):
    """The shared latest-colour-per-room state, behind an interface.

    This is the boundary worth isolating. The whole design rests on workers being
    stateless and interchangeable, which means the colour state lives outside
    them, which means the logic must not know whether that is a dict in this
    process or Redis across the datacenter. Depending on this Protocol is what
    makes the in-memory adapter and a future Redis one interchangeable, and what
    lets the enrichment logic be tested with no infrastructure at all.

    The store is small and bounded by construction: the brief fixes the room and
    camera counts in advance, so there are at most about 100 entries and no
    eviction policy is needed.

    Implementations must tolerate concurrent get and set from multiple threads.
    Reads outnumber writes by roughly 5000 to 1 (500 frames/s against 0.1 colour
    updates/s), and a reader is always allowed to see the previous colour: the
    rule is latest by processing time, so a colour change is only ever a few
    milliseconds of staleness against an interval of 10-20 minutes.
    """

    def get(self, room_id: str) -> Color | None:
        """Return the room's current colour, or None before its first update."""
        ...

    def set(self, room_id: str, color: Color) -> None:
        """Record a room's new colour, replacing any previous one."""
        ...


class Publisher(Protocol):
    """Publishes enriched frames onward, keyed so ordering survives the last hop.

    Assumed guarantees of the underlying pub/sub software, per the brief's rule 4:

    - At-least-once delivery. A rare duplicate is acceptable. Note that a
      duplicate delivered later may pick up a different colour than its first
      copy, so the two copies can genuinely differ; the brief permits this.
    - Ordering preserved per partition key. This is why every publish takes an
      explicit key. Frames are keyed by cameraId on the way in AND on the way
      out, because the no-reordering rule is end to end and has to survive the
      hop to the Inference Service, not just the hop into this service.

    Both a sync and an async form are declared, as rule 4 asks for. Only the sync
    form is exercised by the worker and its tests; the async form is a placeholder
    documenting that a real client would offer one.
    """

    def publish(self, topic: str, key: str, value: Message) -> None:
        """Publish one message to a topic under a partition key."""
        ...

    async def apublish(self, topic: str, key: str, value: Message) -> None:
        """Async form of publish, with identical semantics."""
        ...


class Subscriber(Protocol):
    """Delivers messages from a topic to a handler.

    Assumed guarantees, alongside those on Publisher:

    - The colours topic is log-compacted, so it retains the latest colour per
      room. A restart can therefore repopulate the whole colour state by replaying
      a tiny topic, and a replay yields the current colour rather than an old one.
    - Delivery within a partition is sequential. The handler for a given key is
      never called concurrently with itself, which is what preserves per-camera
      order once frames are keyed by cameraId.

    Publish-before-ack contract: a handler returning normally acks its message,
    and a handler raising causes redelivery. So a handler must finish publishing
    its output before it returns. Getting this order wrong turns at-least-once
    into at-most-once, because a crash between the ack and the publish loses the
    frame with no redelivery to recover it.

    A handler is also responsible for never letting an exception escape for a
    message that cannot succeed on retry. A malformed frame redelivered forever
    would block its partition, which is far worse than dropping it: the brief
    permits rare drops but the system must keep flowing.
    """

    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler to receive every message on a topic."""
        ...

    async def asubscribe(self, topic: str, handler: AsyncMessageHandler) -> None:
        """Async form of subscribe, with identical semantics."""
        ...
