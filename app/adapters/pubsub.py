"""A stubbed pub/sub client standing in for the black-box broker, in memory."""

from collections import defaultdict
from dataclasses import dataclass, field

from app.services.ports import AsyncMessageHandler, Message, MessageHandler

_ASYNC_NOT_IMPLEMENTED = (
    "The async client methods are declared to document that a real pub/sub "
    "client offers them. This reference implementation exercises only the sync "
    "path, which is the one the worker uses."
)


@dataclass
class _Pending:
    """One published message awaiting acknowledgement, with its delivery count."""

    key: str
    value: Message
    attempts: int = field(default=0)


class InMemoryPubSub:
    """An in-process broker that behaves the way the design assumes a real one does.

    The broker itself is a black box in this design, so rather than depend on any
    one product this stub implements the guarantees the design was written
    against. They are the ones a Kafka-style broker actually provides:

    - At-least-once delivery. A message is only removed once its handler has
      returned. A handler that raises leaves the message pending, so it is
      redelivered. That makes an occasional duplicate possible, which the brief
      permits, and makes a silent loss impossible.
    - Ordering preserved per partition key. A key's next message is not delivered
      until its previous one has been acknowledged, so the messages for one key
      are never reordered or overtaken. Different keys make progress
      independently, which is what lets the work spread across workers.
    - A log-compacted colours topic. Compaction keeps only the latest colour per
      room, so a restart can rebuild the entire colour state by replaying a topic
      of at most 100 messages, and a replay yields current colours rather than a
      history of stale ones. Nothing in this stub needs to model that: it holds
      only what has not yet been consumed, which is the same shape from the
      consumer's side.

    Publish-before-ack falls out of this: a handler that publishes its output and
    then returns has already published by the time its input is acknowledged. A
    handler that returned before publishing would turn at-least-once into
    at-most-once, because a crash in between would lose the message with no
    redelivery to recover it.

    One handler per topic, which is all the worker needs. Delivery is driven
    explicitly by deliver_pending rather than by a background thread, so tests
    and the worker stay in control of when work happens and never race.
    """

    def __init__(self) -> None:
        self._pending: dict[str, list[_Pending]] = defaultdict(list)
        self._handlers: dict[str, MessageHandler] = {}

    def publish(self, topic: str, key: str, value: Message) -> None:
        """Append a message to a topic under a partition key.

        The key is not optional, because ordering is only ever guaranteed within
        a key. Frames are keyed by cameraId on the way in and on the way out: the
        no-reordering rule is end to end, so it has to survive the last hop to
        the Inference Service as well as the first hop into this service.
        """
        self._pending[topic].append(_Pending(key=key, value=value))

    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register the handler that will receive this topic's messages.

        Registering does not deliver anything. Delivery happens in
        deliver_pending, so a caller can subscribe to several topics first and
        then let them all run.
        """
        self._handlers[topic] = handler

    def deliver_pending(self, max_attempts: int = 3) -> int:
        """Deliver what is pending until nothing more can make progress.

        Returns the number of messages acknowledged.

        This is the drainable entry point that keeps the stub testable. A real
        broker delivers forever in the background; a test needs delivery to end,
        so this makes repeated passes and stops as soon as a pass attempts
        nothing at all. Because every message is capped at max_attempts
        deliveries per call, that always happens.

        Within a pass, messages are offered in publish order, and a key whose
        message has just failed is skipped for the rest of the pass. That is what
        enforces per-key ordering under failure: a camera's later frame can never
        overtake the earlier one that is still being retried. Other keys carry on
        unaffected.

        A handler returning normally acknowledges its message and it is removed.
        A handler raising does not, so the message stays pending and is offered
        again on the next pass. A message that exhausts max_attempts stays
        pending for a later call rather than being dropped, so nothing is lost
        here. That is head-of-line blocking, and it is exactly why a handler must
        never raise on a message that can never succeed: the worker drops
        malformed frames rather than letting them block their partition forever.
        """
        acknowledged = 0

        while True:
            attempted = False

            # Snapshot both levels: a handler may publish while it runs, and in
            # the worker it always does, which would otherwise mutate what we
            # are iterating. Anything it publishes is picked up on a later pass.
            for topic, messages in list(self._pending.items()):
                handler = self._handlers.get(topic)
                if handler is None:
                    continue

                blocked: set[str] = set()
                delivered: set[int] = set()

                for message in list(messages):
                    if message.key in blocked or message.attempts >= max_attempts:
                        blocked.add(message.key)
                        continue

                    message.attempts += 1
                    attempted = True
                    try:
                        handler(message.value)
                    except Exception:
                        blocked.add(message.key)
                    else:
                        delivered.add(id(message))

                if delivered:
                    self._pending[topic] = [
                        message
                        for message in self._pending[topic]
                        if id(message) not in delivered
                    ]
                    acknowledged += len(delivered)

            if not attempted:
                break

        for messages in self._pending.values():
            for message in messages:
                message.attempts = 0

        return acknowledged

    async def apublish(self, topic: str, key: str, value: Message) -> None:
        """Declared so the transport shape matches a real client; never called."""
        raise NotImplementedError(_ASYNC_NOT_IMPLEMENTED)

    async def asubscribe(self, topic: str, handler: AsyncMessageHandler) -> None:
        """Declared so the transport shape matches a real client; never called."""
        raise NotImplementedError(_ASYNC_NOT_IMPLEMENTED)
