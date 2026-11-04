"""queues - zero-loss async queue SDK.

One class does the work::

    from queues import Queues

    q = Queues()                       # maxsize=1024*512, thresholds built-in
    await q.publish(message)           # unnamed: memory path, never dropped
    await q.publish(message, name="audit")   # named: pressure-gated spill
    message = await q.consume()              # backpressure-aware receive
    message = await q.consume(name="audit")  # auto-drains disk spill first

Pressure semantics (memory/disk watermarks, 0-100):

- disk hard line  -> named messages soak in RAM, spill resumes when disk recovers
- memory soft line -> named messages spill to disk as batch-*.jsonl, FIFO drain
- queue full      -> publish awaits space instead of raising; queue-head never evicted

Zero-loss promise: messages are only lost if memory AND disk are exhausted.
Thresholds are constructor arguments, never config files. Import name is
``queues``; the PyPI distribution is ``queues-sdk``.
"""

from ._version import __version__
from .core import Queues
from .errors import QueuesError

__all__ = ["Queues", "QueuesError", "__version__"]
