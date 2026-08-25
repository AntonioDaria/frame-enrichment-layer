"""The default ColorStore: a dict, because the state is small enough to be one."""

from app.services.ports import Color


class InMemoryColorStore:
    """Holds the latest colour per room in a plain dict.

    This is the whole of the system's state. The brief fixes the room and camera
    counts in advance and gives each room exactly one light, so the store holds at
    most about 100 entries of three floats each: a few kilobytes, bounded by
    construction. That is why there is no eviction policy, no size limit and no
    TTL. Nothing here can grow without a new room being installed.

    No lock, deliberately. Reads outnumber writes by roughly 5000 to 1 (500 frames
    a second against 0.1 colour updates a second), and dict get and set are atomic
    under the GIL, so a reader can never observe a half-written entry. The worst a
    concurrent reader can see is the previous colour, and the rule is the latest
    colour by processing time, so that is a few milliseconds of staleness against
    a colour that changes every 10 to 20 minutes. A lock would add contention on
    the hot path to prevent something the design already tolerates.

    In production this adapter is swapped for a Redis-backed one so that
    horizontally scaled workers share the state. Nothing else changes: the logic
    depends on the ColorStore Protocol, not on this class.
    """

    def __init__(self) -> None:
        self._colors: dict[str, Color] = {}

    def get(self, room_id: str) -> Color | None:
        """Return the room's current colour, or None before its first update.

        None is a normal answer, not a failure: it is what every room reports
        until its light first reports in, and the caller emits the frame anyway.
        """
        return self._colors.get(room_id)

    def set(self, room_id: str, color: Color) -> None:
        """Make this the room's current colour, discarding any previous one.

        Only the latest colour is ever needed, so there is no history to keep and
        overwriting is the entire write path.
        """
        self._colors[room_id] = color
