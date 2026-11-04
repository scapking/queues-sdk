# 03 — Performance & latency profile

Measured on CPython 3.14 free-threaded, Linux, single process, in-process alternation against the upstream engine implementation.

## Throughput

| Path | Throughput |
|---|---|
| `publish`, unnamed, normal watermarks | ~820k ops/s |
| `publish`+`consume` pair (end-to-end TPS) | ~72k pairs/s |
| named spill on soft line | ~140k ops/s |
| `drain` refill | ~356k ops/s |

Design points behind these numbers:

- publish without a waiter: lock-free `deque.append`, no locks, no event-loop round trips.
- publish with a waiter: `future.set_result` handoff — the message never touches the buffer.
- unnamed publishes bypass the pressure gate coroutine entirely.
- refill reads a whole spilled file at once and deserialises in one pass.

## Latency profile: steady vs paused

The hot path is **steady-state**: no timers, no periodic flushes, allocations are short-lived objects reclaimed by reference counting, so there is no GC-shaped pause built into the design.

The only **episodic** pause source is batched spill: measured median `publish` latency on the soft line is ~10 µs, with a ~1-4 ms max spike on the batch boundary (one synchronous `open`+`write` per 1024 lines). Under the hard line there are no writes at all — messages soak in RAM.

Backpressure (`queue full`) is event-driven suspension, not polling and not a pause: producers resume exactly when a consumer frees one slot.

## Tuning

- `spill_batch` — lower it to shorten the per-file write spike; raise it to amortise fewer, bigger writes.
- `maxsize` — raise it to absorb bursts fully in RAM.
- Watermark thresholds (`memory_max`/`disk_max`) — lower them to enter the safe paths earlier; they are constructor arguments, tuned per deployment, never state.
