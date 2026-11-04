# 02 — API reference

## `Queues`

```python
Queues(maxsize=1024*1024, memory_max=95, disk_max=95, spill_path="~/.local/queues/spill/", spill_batch=1024, watermarks=None) -> Queues
```

## `async publish(message, name=None) -> None`

- `name=None`: memory fast path; if the buffer is full the producer awaits space (backpressure, zero loss, queue head never evicted).
- `name=str`: goes through the pressure gate. On the soft line the message is buffered and spilled to disk in batches; on the hard line it soaks in RAM until the disk recovers.
- The five service log lanes are for the host application to use directly; `publish` does not touch them.

## `async consume(name=None) -> Any`

1. Buffer non-empty: pop the head, wake one space-waiting producer if there is room.
2. `name` given and spill residue exists: bounded `drain` into the buffer, then pop.
3. Otherwise: attach a waiter future and suspend; `publish` hands the message over directly.

Cancellation while waiting removes the waiter and re-raises `CancelledError` — a cancelled consume never silently returns a fake `None` mixed with real data.

## `async drain(name, target, limit=None) -> int`

Bounded FIFO refill from disk into `target` (a list is typical). Spilled files are read oldest-first, each deleted after it is fully consumed (at-least-once on crash). Corrupt lines quarantine the whole file as `.bad` instead of blocking the stream.

## `async discard(name) -> int`

Wipes `pending`, `draining` and `checked` state plus the on-disk directory; the return value is the total number of messages discarded (pending lines + spill file lines) for reconciliation.

## `state() -> str`

`"normal"` / `"soft"` / `"hard"` from the injected watermarks; any provider failure degrades to `"normal"`.

## Exceptions

- `QueuesError` — explicit API failure surfaced to callers.

## Error logging

Internal failures go through `__log_error` into the `queues` standard-library logger at `ERROR` level with a structured dict (id, UTC time, method, type, value, file/line, pid/thread, traceback). Nothing is swallowed silently.
