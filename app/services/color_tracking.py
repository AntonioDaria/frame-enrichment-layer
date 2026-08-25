"""Colour tracking: record a light's new colour as the current one for its room."""

import structlog

from app.schemas import ColorUpdate
from app.services.ports import ColorStore

logger = structlog.get_logger(__name__)


def apply(update: ColorUpdate, color_store: ColorStore) -> None:
    """Make this update the room's current colour.

    This is the entire write side of the system's state: about 0.1 updates a
    second across all 100 rooms, against 500 frame reads a second. Being the cold
    path is what makes it affordable to validate and log here, and pointless to
    optimise.

    Last write wins by arrival order, with no timestamp guard. The rule is the
    latest colour by processing time, so the update that arrives second is the
    current one by definition, even if its own timestamp is older. Comparing
    timestamps here would quietly replace that rule with a different one, and it
    would also mean trusting the producers' clocks over the broker's ordering.

    An out-of-range component is stored anyway. A colour outside the usual
    0.0-1.0 is still a real reading from a real light, and refusing it would
    leave the room pinned to its previous colour indefinitely, silently staling
    every subsequent frame. Logging the oddity keeps it visible without throwing
    data away.
    """
    if any(not 0.0 <= component <= 1.0 for component in update.new_color):
        logger.warning(
            "Colour component outside the usual 0.0-1.0 range; storing it anyway",
            room_id=update.room_id,
            color=update.new_color,
        )

    color_store.set(update.room_id, update.new_color)
