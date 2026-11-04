# Changelog

## 0.1.0 — 2026-11-04

- Initial release.
- `Queues` core: lock-free deque buffer, waiter future handoff, backpressure waiting on full capacity.
- Pressure gate: memory soft line (spill to disk), disk hard line (soak in RAM), auto drain FIFO, poison-file quarantine (`.bad`), discard with drop accounting.
- Five built-in service log lanes (1024*4 each).
- Injectable watermark provider; default reads `/proc/meminfo` + `statvfs("/")`.
- Zero dependencies; stdlib only. Python >= 3.10.
