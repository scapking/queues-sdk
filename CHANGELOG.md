# Changelog

## 0.3.0 — 2026-11-05

- Defaults retuned: `maxsize` 1024*512 → 1024*1024, `memory_max`/`disk_max` 99 → 95. `spill_path`, `spill_batch` (1024) and `watermarks` (built-in `/proc` reader) unchanged — bare `Queues()`/`Queue()` now runs the tuned engine profile.

## 0.2.1 — 2026-11-04

- `Queue` alias for `Queues` (asyncio.Queue-style naming): `from queues import Queue` — same class, same six configurable parameters. `Queues` stays valid.

## 0.2.0 — 2026-11-04

- **BREAKING**: import module renamed `queues_sdk` → `queues` (the PyPI distribution name stays `queues-sdk`). Update your imports: `from queues import Queues`.
- Default spill directory renamed `~/.local/queues/spill/` for consistency; move existing `batch-*.jsonl` files by hand if you had any.
- Internal stdlib logger renamed `queues_sdk` → `queues`.

## 0.1.2 — 2026-11-04

- Editor support: `Queues` is now `Generic[T]` — `publish(message: T)` / `consume() -> T`; every public method carries full parameter/return annotations and Chinese docstrings with `Args` sections, so signature help and hover docs behave like `asyncio.Queue`.
- Ships a PEP 561 `py.typed` marker so type checkers trust the inline annotations.

## 0.1.1 — 2026-11-04

- Fixed a lost-wakeup deadlock in `publish`: after resuming from the full-capacity wait, a producer appended to the buffer without checking registered consumers — a lone producer finishing after a resume could leave a suspended consumer waiting forever (reproducible at maxsize=1) and let later handoffs overtake messages already in the buffer. Publish now hands off directly to a registered waiter after acquiring space and chains the freed slot to the next space-waiting producer. FIFO and liveness verified with a strict two-task pipeline regression test.
- Removed duplicated dead `return` statements in `publish`/`consume` error handlers.

## 0.1.0 — 2026-11-04

- Initial release.
- `Queues` core: lock-free deque buffer, waiter future handoff, backpressure waiting on full capacity.
- Pressure gate: memory soft line (spill to disk), disk hard line (soak in RAM), auto drain FIFO, poison-file quarantine (`.bad`), discard with drop accounting.
- Five built-in service log lanes (1024*4 each).
- Injectable watermark provider; default reads `/proc/meminfo` + `statvfs("/")`.
- Zero dependencies; stdlib only. Python >= 3.10.
