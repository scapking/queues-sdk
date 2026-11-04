import pytest

from queues import Queues
from queues.watermarks import default_watermarks





@pytest.fixture
def q() -> Queues:
    return Queues(maxsize=1024*4, watermarks=lambda: (int(0), int(0)))





@pytest.fixture
def watermarks():
    holder = {"memory": int(0), "disk": int(0)}
    return holder, lambda: (int(holder["memory"]), int(holder["disk"]))
