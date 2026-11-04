# Changelog

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
