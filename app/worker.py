"""Wiring: consume frames and colours, enrich, publish, and survive bad messages."""

from typing import Any

import structlog
from pydantic import ValidationError

from app.config import Config
from app.schemas import ColorUpdate
from app.services import color_tracking
from app.services.enrichment import enrich
from app.services.ports import Broker, ColorStore, Message

logger = structlog.get_logger(__name__)


def _is_identifier(value: Any) -> bool:
    """Say whether a field can be used as a room or camera id.

    Kept separate so the guard reads as one rule applied twice rather than as
    four boolean clauses inline.
    """
    return isinstance(value, str) and value != ""


class EnrichmentWorker:
    """Consumes frames, attaches each room's current colour, publishes them on.

    The worker owns no state of its own beyond a small set of rooms it has
    already warned about. Everything that matters lives in the ColorStore, which
    is shared, so any worker can take any frame and a worker that restarts or is
    rebalanced needs no recovery. That is what makes the horizontal scaling
    clean.

    Processing is strictly sequential within a partition, and deliberately so.
    The per-frame work is a single dict lookup, while the per-frame bytes are
    100-500 KB: the bottleneck is bandwidth, not CPU, so a thread pool would buy
    no throughput. What it would cost is the one ordering rule the brief sets,
    because two frames from the same camera handled concurrently can finish out
    of order. Sequential per partition is therefore free here, and anything else
    is a regression.
    """

    def __init__(
        self, broker: Broker, color_store: ColorStore, config: Config
    ) -> None:
        self._broker = broker
        self._color_store = color_store
        self._config = config
        self._rooms_warned_about: set[str] = set()

        broker.subscribe(config.frames_topic, self._handle_frame)
        broker.subscribe(config.colours_topic, self._handle_colour)

    def _handle_frame(self, frame: Message) -> None:
        """Enrich one frame and publish it, or drop it if it cannot be enriched.

        This handler must never raise. An exception escaping a consumer does not
        just lose one message: it blocks the whole partition behind it, so a
        single malformed frame would stall every later frame from those cameras.
        Dropping is the right trade, and the brief allows rare drops explicitly.

        The failure log names the fields that are present, never the frame body.
        The body is up to 500 KB of base64, and logging it once per bad message
        would turn a small problem into an outage of its own.
        """
        room_id = frame.get("roomId")
        camera_id = frame.get("cameraId")

        if not _is_identifier(room_id) or not _is_identifier(camera_id):
            logger.error(
                "Dropping malformed frame: roomId and cameraId must be "
                "non-empty strings",
                room_id=room_id if isinstance(room_id, str) else None,
                camera_id=camera_id if isinstance(camera_id, str) else None,
                fields=sorted(frame),
            )
            return

        enriched = enrich(frame, self._color_store)

        if enriched["color"] is None and room_id not in self._rooms_warned_about:
            self._rooms_warned_about.add(room_id)
            logger.warning(
                "No colour known for room yet; emitting frames with a null colour",
                room_id=room_id,
            )

        self._broker.publish(
            self._config.enriched_topic, key=camera_id, value=enriched
        )

    def _handle_colour(self, message: Message) -> None:
        """Record a light's new colour, or drop the message if it is malformed.

        This is the cold path, about 0.1 messages a second, so unlike the frame
        path it can afford full validation. It is also the path where a bad
        message does the most damage: a wrong colour stored here would be
        attached to every subsequent frame in that room, so it is worth rejecting
        rather than storing something unusable.
        """
        try:
            update = ColorUpdate.model_validate(message)
        except ValidationError:
            logger.error(
                "Dropping malformed colour update",
                fields=sorted(message) if isinstance(message, dict) else None,
            )
            return

        color_tracking.apply(update, self._color_store)
