# queues-sdk

**Zero-loss async queue SDK for Python.** Direct waiter handoff, backpressure waiting, pressure-gated spill to disk with FIFO drain. No network, no dependencies, stdlib only.

Import name is `queues`; the PyPI distribution is `queues-sdk`.

## Why

Plain `asyncio.Queue` either blocks forever when full or drops messages if you cap it. Real pipelines need a third behaviour: **never lose, wait when possible, spill to disk when memory is tight, and soak in RAM when disk is tight.** queues-sdk implements exactly that on top of a lock-free deque + future handoff design.

## Install

```bash
pip install queues-sdk
```

## Quickstart

```python
import asyncio
from queues import Queues

q = Queues()                     # maxsize=1024*512, thresholds built-in

async def main():
    await q.publish({"event": "signup"})           # unnamed: memory path
    message = await q.consume()                    # backpressure-aware receive

    await q.publish({"event": "audit"}, name="audit")   # named: pressure-gated
    message = await q.consume(name="audit")             # auto-drains disk spill

asyncio.run(main())
```

## Zero-loss semantics

| Condition | Behaviour | Lost messages |
|---|---|---|
| normal watermarks, queue not full | lock-free deque append / future handoff | none |
| queue full | publish **awaits space** (backpressure); queue-head never evicted | none |
| memory soft line (>= memory_max) | named messages spill to disk as `batch-*.jsonl`, FIFO drain on consume | none |
| disk hard line (>= disk_max) | named messages soak in RAM, spill resumes when disk recovers | none |
| memory AND disk exhausted | no retreat | only here |

## API

- `Queues(maxsize=1024*512, memory_max=99, disk_max=99, spill_path="~/.local/queues/spill/", spill_batch=1024, watermarks=None)`
- `await publish(message, name=None) -> None` — unnamed skips the pressure gate entirely (hot path)
- `await consume(name=None) -> Any` — auto-drains the named spill buffer before waiting
- `await drain(name, target_list, limit=None) -> int` — bounded FIFO refill from disk
- `await discard(name) -> int` — wipe a spill buffer, returns dropped-message count
- `state() -> "normal" | "soft" | "hard"` — current pressure mode
- Five pre-built `asyncio.Queue` log lanes (`almightydb_service_logger_warning/info/debug/error/store`, each 1024*4) ship inside every instance

Watermarks are injected: pass `watermarks=lambda: (memory_pct, disk_pct)`; the default provider reads `/proc/meminfo` + `statvfs("/")` on Linux and reports `(0, 0)` elsewhere.

## Performance

Measured on CPython 3.14 free-threaded, Linux, single process:

| Mode | plain asyncio | uvloop |
|---|---|---|
| pipelined producer+consumer, buffered (maxsize>=1024) | ~950k-970k pairs/s | ~950k-970k pairs/s |
| pipelined, tight backpressure (maxsize=8) | ~480k pairs/s | ~600k pairs/s |
| strict two-task ping-pong (maxsize=1) | ~165k pairs/s | ~233k pairs/s |
| forced per-message-yield ping-pong benchmark | ~135k pairs/s | ~204k pairs/s |

The last row is the worst case: every message forces two full event-loop task switches. For reference, a bare `asyncio.Future` handoff with no queue mechanism at all measures ~151k / ~241k pairs/s there — the queue's own overhead is ~11-16%; the rest is the event loop's task-switch cost. Buffer operations (`deque` append/popleft) run at ~11M ops/s.

Spill writes are batched (1024 lines per file): expect one ~1-4 ms disk pause per batch under the soft line.

## Docs

- [Getting started](docs/01-getting-started.md)
- [API reference](docs/02-api-reference.md)
- [Performance & latency profile](docs/03-performance.md)

## License

MIT
