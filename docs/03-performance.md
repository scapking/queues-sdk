# 03 — Performance & latency profile

Measured on CPython 3.14 free-threaded, Linux, single process, 3 rounds taking the best run.

## Throughput by mode

| Mode | plain asyncio | uvloop |
|---|---|---|
| pipelined producer+consumer, buffered (maxsize>=1024) | ~950k-970k pairs/s | ~950k-970k pairs/s |
| pipelined, tight backpressure (maxsize=8) | ~480k pairs/s | ~600k pairs/s |
| strict two-task ping-pong (maxsize=1) | ~165k pairs/s | ~233k pairs/s |
| forced per-message-yield ping-pong benchmark | ~135k pairs/s | ~204k pairs/s |

## How to read these numbers

The last row is the **worst case** and the one most often quoted: producer and consumer alternate one message at a time, each message paying two full event-loop task switches. A bare `asyncio.Future` handoff with no queue mechanism at all measures ~151k (plain) / ~241k (uvloop) pairs/s in that mode — the queue's own overhead is ~11-16% of the ping-pong cost; the remaining ~85% is the event loop's task-switch tax, not the mechanism.

The pipelined rows are the realistic shape: producer and consumer run concurrently through the bounded buffer, no artificial yields. With room to buffer, publish and consume mostly run inline (`deque` append/popleft at ~11M ops/s) and pairs flow at ~950k-970k pairs/s. At `maxsize=1` every pair goes through the full backpressure handshake and still sustains 165k-233k pairs/s.

Named-message spill paths (measured, plain asyncio): named spill on the soft line ~140k ops/s, drain refill ~356k ops/s.

## Latency profile: steady vs paused

The hot path is **steady-state**: no timers, no periodic flushes, allocations are short-lived objects reclaimed by reference counting, so there is no GC-shaped pause built into the design.

The only **episodic** pause source is batched spill: median `publish` latency on the soft line is ~10 µs, with a ~1-4 ms max spike on the batch boundary (one synchronous `open`+`write` per 1024 lines). Under the hard line there are no writes at all — messages soak in RAM.

Backpressure (`queue full`) is event-driven suspension, not polling and not a pause: producers resume exactly when a consumer frees one slot — or, since 0.1.1, hand the message straight to an already-waiting consumer and pass the freed slot to the next producer (this also removed a lost-wakeup deadlock present in 0.1.0).

## Tuning

- `spill_batch` — lower it to shorten the per-file write spike; raise it to amortise fewer, bigger writes.
- `maxsize` — raise it to absorb bursts fully in RAM (the biggest single lever on pipelined throughput).
- Watermark thresholds (`memory_max`/`disk_max`) — lower them to enter the safe paths earlier; they are constructor arguments, tuned per deployment, never state.
- Running under uvloop lifts every waiting-handoff mode by ~25-55%; the buffered pipeline is already inline-bound and unaffected.
