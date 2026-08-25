"""A runnable illustration of the enrichment layer against the in-memory stub.

This is not the production entry point. A deployed worker consumes from a real
broker, which pushes messages to it continuously and blocks when there are none.
Here everything is in one process and delivery is driven explicitly, so the whole
cold-start story fits in one readable script and finishes on its own.
"""

import structlog

from app.adapters.color_store import InMemoryColorStore
from app.adapters.pubsub import InMemoryPubSub
from app.config import Config
from app.services.ports import Message
from app.worker import EnrichmentWorker

logger = structlog.get_logger(__name__)


def main() -> None:
    """Show the cold-start rule end to end: a null colour, then a real one.

    Draining between each publish is the point rather than an implementation
    detail. The rule is the latest colour by processing time, so which colour a
    frame gets depends on what has been processed before it arrives, and that is
    exactly what the three steps below demonstrate.
    """
    config = Config.from_env()
    broker = InMemoryPubSub()
    EnrichmentWorker(broker=broker, color_store=InMemoryColorStore(), config=config)

    def show(enriched: Message) -> None:
        logger.info(
            "Enriched frame published",
            room_id=enriched["roomId"],
            camera_id=enriched["cameraId"],
            color=enriched["color"],
        )

    broker.subscribe(config.enriched_topic, show)

    def frame(timestamp: str) -> Message:
        return {
            "roomId": "room-1",
            "cameraId": "camera-a",
            "frame": "ZnJhbWUtYnl0ZXM=",
            "timestamp": timestamp,
        }

    logger.info("A frame arriving before the room's light has reported in")
    broker.publish(config.frames_topic, key="camera-a", value=frame("10:00:00"))
    broker.deliver_pending()

    logger.info("The room's light reports a colour")
    broker.publish(
        config.colours_topic,
        key="room-1",
        value={
            "roomId": "room-1",
            "newColor": [0.1, 1.0, 0.5],
            "timestamp": "10:00:05",
        },
    )
    broker.deliver_pending()

    logger.info("A frame arriving after the colour is known")
    broker.publish(config.frames_topic, key="camera-a", value=frame("10:00:10"))
    broker.deliver_pending()


if __name__ == "__main__":
    main()
