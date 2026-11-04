# 01 — Getting started

## Requirements

- Python >= 3.10
- No runtime dependencies.

## Install

```bash
pip install queues-sdk
```

## Create a queue

```python
from queues import Queues

q = Queues()
```

Constructor arguments (all optional, thresholds are **built into the instance**, never read from config files):

| Argument | Default | Meaning |
|---|---|---|
| `maxsize` | `1024*512` | in-memory buffer capacity |
| `memory_max` | `99` | memory soft line, 0-100 |
| `disk_max` | `99` | disk hard line, 0-100 |
| `spill_path` | `~/.local/queues/spill/` | spill root directory |
| `spill_batch` | `1024` | lines per spilled batch file |
| `watermarks` | `None` | provider returning `(memory_pct, disk_pct)` |

## Publish and consume

```python
await q.publish(message)                  # unnamed: memory path
await q.publish(message, name="audit")    # named: pressure-gated spill
message = await q.consume()               # memory path first, then waits
message = await q.consume(name="audit")   # auto-drains the spill buffer first
```

- Unnamed calls skip the pressure gate entirely — that is the hot path.
- Named calls route through the gate: `spill` when a watermark is crossed, `pass` otherwise.
- `consume` blocks with a future handoff; producers deliver messages directly without touching the buffer.

## Watermark injection

```python
q = Queues(watermarks=lambda: (75, 40))   # memory 75%, disk 40%
```

The default provider computes both percentages from `/proc/meminfo` (`MemTotal`/`MemAvailable`) and `statvfs("/")`; on non-Linux platforms it reports `(0, 0)`.

## Next

- [API reference](02-api-reference.md)
- [Performance & latency profile](03-performance.md)
