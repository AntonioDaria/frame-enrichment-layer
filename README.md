# Frame Enrichment Layer

This is my design for the part of the system marked by the green circle in the
brief: the layer that sits between the producers (cameras and room lights) and
the consumer (the Inference Service), and enriches every camera frame with the
latest known light colour for its room.

The brief asks for a design (this README) plus code for any new service the
design needs. My design needs exactly one new service - a stateless
**enrichment worker** - so that is the piece I actually build and test. Every
other component (the message broker, a colour cache) is a black box I describe
here rather than implement, which is also what the brief allows.

## The problem in one line

Two message streams arrive with very different shapes, and I have to join them:
a **high-volume frame stream** (one frame per camera per second) and a
**low-volume, sporadic colour stream** (one update per room every 10-20
minutes). Each frame must go out tagged with the most recent colour for its
room - latest *by processing time* - while preserving per-camera order and
scaling horizontally.

## Sizing the load first

Before choosing a shape I sized the load, because the bottleneck decides the
design. Taking the ceilings from the brief (100 rooms x 5 cameras, 1 FPS, frames
of 100-500 KB, one light per room changing every 10-20 min):

| Dimension | Number | Reading |
|---|---|---|
| Frame rate | 500 cameras x 1 FPS = **500 frames/s** | low - trivial message count |
| Frame bandwidth | 500 x up to 500 KB = **50-250 MB/s** | **the bottleneck** - fits one fat box, spread for headroom |
| Colour rate | 100 rooms x ~4/hr = **~0.1 msg/s** | negligible |
| Colour state | 100 rooms x one `[r,g,b]` = **a few KB** | fits in memory a million times over |
| Latency | feeds real-time inference | can't buffer frames |

The whole design follows from this table. The per-frame *work* (one colour
lookup) is nothing; the per-frame *bytes* are everything. So the guiding rule is:
**let each frame flow straight through, touch it once to attach a tiny colour,
and never copy, buffer, or route the big blobs where I don't have to.** The
colour state is so small and so slow that keeping it fresh is essentially free.

## Architecture

I kept my usual layered, ports-and-adapters structure so each concern has one
home and the core logic is testable without any broker or network:

    app/
      config.py                # settings from env (topic names, consumer group)
      schemas.py               # pydantic models: FrameIn, ColorUpdate, EnrichedFrame
      services/
        ports.py               # the Protocols: ColorStore, Publisher, Subscriber, Broker
        enrichment.py          # core logic: frame + ColorStore -> enriched frame
        color_tracking.py      # apply a colour update -> ColorStore (latest wins)
      adapters/
        color_store.py         # InMemoryColorStore (the default ColorStore)
        pubsub.py              # stubbed pub/sub client with documented guarantees
      worker.py                # the service: subscribes the frame and colour handlers
      demo.py                  # a runnable illustration, not the production entry point
    tests/
      conftest.py              # a fake ColorStore and a frame builder
      services/
        test_enrichment.py     # the enrichment rule against a fake store
        test_color_tracking.py # the latest-colour rule against a fake store
      test_adapters.py         # the colour store and the pub/sub stub
      test_integration.py      # the whole path, through the real stub
    Makefile

Why this shape:

- **`services/enrichment.py` is the green circle's heart** and is pure: given a
  parsed frame and something that can answer `get(room_id)`, it returns the
  enriched frame. It imports no broker and no cache, so I can unit-test the
  enrichment and the "latest colour" rule against a fake with no infrastructure.
- **`services/ports.py` holds a `ColorStore` Protocol and the transport
  Protocols** (`Publisher`, `Subscriber`, and `Broker` for something that does
  both). These are the two boundaries I most want to isolate and fake, so the
  logic depends on the Protocol, never on Redis or on a concrete broker client.
  This is the same judgement I apply everywhere: put an interface where it buys
  real substitutability (swap in-memory for Redis; fake it in tests) and nowhere
  it would just be indirection.
- **`adapters/` holds the replaceable edges** - an in-memory `ColorStore` and a
  stubbed pub/sub client - so the default project runs and tests with zero
  external services, and a production build swaps the adapter, not the logic.
- **`worker.py` is the service itself.** It subscribes two handlers and does
  nothing else; it never drives delivery. **`demo.py` is a script**, not the
  production entry point: it wires the in-memory pieces together so the whole
  story can be run in one process.

### How a frame flows

1. A frame arrives on the `frames` topic (partitioned by `cameraId`).
2. The worker checks the frame is usable, calls `color_store.get(room_id)`, and
   attaches the result as `"color"`.
3. It publishes the enriched frame, keyed by `cameraId`.

Frames are handled as plain dicts, not as pydantic models. The unavoidable
per-frame cost is **one JSON parse on the way in** (I have to parse to reach
`roomId`, which sits in the same object as the blob) and **one JSON serialize on
the way out**. A model round-trip would add a third and fourth pass over the same
data for nothing, so there is no `FrameIn` construction on this path, and the
base64 blob is never decoded or inspected - it is carried by reference into the
new dict. Pydantic is used only on the colours path, where validation costs
nothing and is worth having.

### How a colour flows

A second handler in the same service reads the `colours` topic and writes
`room_id -> latest colour` into the `ColorStore` (~0.1 writes/s). That is all the
state the system keeps. It is a separate handler rather than a separate service
because it is about twenty lines; what matters is that it writes to the shared
store, not which process it runs in.

## Where the colour state lives

Because workers are horizontally scaled, any worker may handle any frame, so it
needs a way to know a room's current colour. I keep that state in a shared cache
(Redis): a small consumer reads the `colours` topic and writes `roomId -> colour`
into the cache, and every enrichment worker reads it when a frame arrives.

This keeps the workers stateless and interchangeable, which is what makes the
horizontal scaling clean. Any worker can take any frame, and a worker that
restarts or is rebalanced needs no state recovery because it holds no state. The
per-frame read costs effectively nothing at this scale (500 reads/s against a
cache that serves 100k+/s, sub-millisecond in-datacenter), and the colour itself
is only a few bytes per room.

The in-memory `ColorStore` I ship is process-local, which is why the demo runs
both consumers in one process. That is a property of the adapter, not of the
design: a shared Redis-backed store is exactly what lets the colour consumer and
the frame workers be separate, independently scaled processes.

## Partitioning and ordering

The frame stream is **partitioned by `cameraId`**, and the worker republishes
each enriched frame **keyed by `cameraId` as well**. A broker keeps order within
a partition, and a camera maps to exactly one partition, so a camera's frames are
never reordered - which is the one ordering rule the brief sets. Keying the
output matters as much as keying the input: the guarantee has to survive the last
hop to the Inference Service, not just the first hop into this service.

Keying by camera (rather than by room) also spreads a busy room's five cameras
across workers for even load, and it costs nothing here because the colour lookup
does not depend on the frame's partition key.

Within a partition, processing is strictly sequential. The per-frame work is one
dict lookup while the per-frame bytes are the bottleneck, so concurrency inside a
partition would buy no throughput and would cost the ordering guarantee.

## Delivery guarantees I assume

Following rule 4, I design against a generic broker with **at-least-once
delivery** and **ordering preserved per partition key**, which is what
Kafka-style brokers actually provide and what the requirements were written for:

- *At-least-once* means an occasional duplicate frame, which the brief allows
  ("rare drops and duplicates are acceptable"). It is worth being precise about
  what a duplicate means here: a copy delivered later can pick up a *different*
  colour than the first copy did, so the two emitted frames can genuinely differ.
  That is a consequence of stamping the latest colour by processing time, and
  rule 3 permits it.
- *Per-key ordering* gives the no-reordering guarantee for free once frames are
  partitioned by camera.
- The `colours` topic is **log-compacted** (keeps the latest colour per room),
  so the shared cache can be repopulated after a restart by replaying a tiny
  topic, rather than waiting for each room's next colour update.

The stubbed client in `adapters/pubsub.py` implements these guarantees rather
than describing them: nothing is acknowledged until its handler returns, a
handler that raises causes redelivery, and a key's next message is never
delivered until its previous one is acknowledged. It offers sync
`publish`/`subscribe` plus `apublish`/`asubscribe` placeholders, so the design
does not depend on any one broker product.

The worker itself depends only on the abstract transport - subscribe a handler,
publish a message. It never drives delivery: a real broker pushes to the
subscribed handlers, while the demo and the tests drive the in-memory stub
explicitly so they finish instead of blocking.

## Message schemas (preserved at the boundaries)

The producers and the Inference Service are black boxes, so I keep their JSON
exactly. In:

```json
{ "roomId": "room-1", "cameraId": "camera-a", "frame": "<base64>", "timestamp": "2026-06-01T10:00:00" }
{ "roomId": "room-1", "newColor": [0.1, 1.0, 0.5], "timestamp": "2026-06-01T10:00:00" }
```

Out (the frame, unchanged, plus one field):

```json
{ "roomId": "room-1", "cameraId": "camera-a", "frame": "<base64>", "timestamp": "2026-06-01T10:00:00", "color": [0.1, 1.0, 0.5] }
```

## Assumptions

- **Cold start:** before a room's first colour arrives, `get(room_id)` returns
  `None` and the worker emits the frame with `"color": null` rather than dropping
  it. A frame with no colour is more useful to the Inference Service than no
  frame, and it self-corrects on the room's first colour update. I warn **once
  per room**, never per frame: at 500 frames/s, per-frame logging would be 500
  lines/s. There is deliberately no configured room registry and no separate
  "unknown room" error path - a colour I do not have yet and a room I have never
  heard of are the same situation, and both resolve the same way.
- **Latest by processing time**, exactly as stated: colour updates are
  last-write-wins by arrival order, with no timestamp guard. The update that
  arrives second is the current one by definition, even if its own timestamp is
  older; comparing timestamps would quietly replace the brief's rule with a
  different one and would trust the producers' clocks over the broker's ordering.
  Because colours change every 10-20 minutes while frames arrive every second, a
  few milliseconds of staleness around a change needs no event-time buffering or
  windowing. I did not implement a warm-up read on startup, though the
  log-compacted colours topic makes one straightforward if the null window ever
  mattered.
- **Bad messages are dropped, not raised.** An unhandled exception in a consumer
  does not lose one message, it blocks the whole partition behind it, so both
  handlers swallow what cannot succeed. A frame whose `roomId` or `cameraId` is
  missing or is not a non-empty string is logged and dropped by a guard that runs
  before enrichment; the log names the fields that are present and never the
  frame body, which is up to 500 KB. A malformed colour update is validated and
  dropped rather than stored, because a bad colour would otherwise be attached to
  every subsequent frame in that room.
- The frame's base64 blob is treated as opaque bytes - the enrichment layer never
  decodes it.

## Meeting 500 frames/s

The brief asks the layer to sustain at least 500 frames/s, and the two halves of
that number are worth separating. The **message rate** is trivial: enriching a
frame is one dict lookup, one shallow copy and one publish, and the throughput
test drives a couple of thousand frames through the stub in a few milliseconds -
ample headroom. The **bandwidth** is the real constraint at 50-250 MB/s, and it
is a property of the network and the deployment rather than of this process, so
it is argued here rather than asserted in a test. Note also that 500/s is the
aggregate across all workers and partitions, so a single worker only ever has to
carry a fraction of it.

If that bandwidth became the limit, the move would be to pass the frame by
reference - producers write the blob to object storage and the message carries a
URI - which keeps the bytes off the broker while keeping the queue and the JSON
schemas at the boundaries exactly as they are.

## Scope

Implemented in Python with tests: the enrichment logic, the colour-tracking
logic, the `ColorStore` and transport Protocols with an in-memory adapter for
each, and the worker that wires them together. Described here but not built: the
broker itself, a Redis-backed `ColorStore`, and deployment - per the brief, no
Kubernetes manifests.

## Running

    make install     # uv sync
    make demo        # uv run python -m app.demo
    make test        # uv run pytest
    make lint        # uv run ruff check .
    make check       # lint, then test

Or directly:

    uv sync
    uv run python -m app.demo

`app/demo.py` publishes a frame before any colour has arrived (which comes out
with `"color": null`), then a colour for that room, then a second frame (which
comes out carrying it) - the cold-start rule and the processing-time rule, both
visible in six lines of output.

## Tests

    uv run pytest

- **Unit tests** cover the enrichment function and the latest-colour rule against
  a fake `ColorStore`: cold start, a colour change landing between two frames,
  the input dict never being mutated, and the blob being carried through
  untouched and never decoded.
- **Adapter tests** cover the two guarantees the stub exists to provide -
  per-key ordering (including when a key is failing and must not be overtaken)
  and redelivery of a message whose handler raised.
- **Integration tests** drive the real worker, store and stub end to end: the
  cold-start-then-colour path, per-camera order across the whole path, a
  malformed frame and a malformed colour each being dropped without taking the
  loop down, and the per-message throughput described above.

All of it is offline and deterministic. The guiding principle is the one I always
use: test behaviour, not the framework.
