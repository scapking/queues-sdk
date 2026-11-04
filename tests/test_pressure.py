import asyncio

from queues_sdk import Queues
from queues_sdk.watermarks import default_watermarks





async def test_soft_spill_and_drain_fifo():
    holder = {"memory": int(100), "disk": int(0)}
    q = Queues(maxsize=1024*4, spill_batch=1024, watermarks=lambda: (int(holder["memory"]), int(holder["disk"])))
    for i in range(3): await q.publish({"m": i}, name="probe")
    assert sum(len(v) for v in q.pending.values()) == 3
    holder["memory"] = 0
    drained = []
    pulled = await q.drain("probe", drained)
    assert int(pulled) == 3 and drained == [{"m": 0}, {"m": 1}, {"m": 2}]
    dropped = await q.discard("probe")
    assert int(dropped) == 0 and not q.pending.get("probe")





async def test_hard_soaks_in_ram_without_disk_write():
    holder = {"memory": int(0), "disk": int(100)}
    q = Queues(maxsize=1024*4, spill_batch=1024, watermarks=lambda: (int(holder["memory"]), int(holder["disk"])))
    for i in range(1103): await q.publish({"m": i}, name="probe")
    assert sum(len(v) for v in q.pending.values()) == 1103
    import pathlib
    directory = pathlib.Path(q.pressure_spill_path).expanduser() / "probe"
    assert not (directory.is_dir() and any(directory.glob("*.jsonl")))
    holder["disk"] = 0
    drained = []
    pulled = await q.drain("probe", drained)
    assert int(pulled) == 1103 and [x["m"] for x in drained][:3] == [0, 1, 2]
    await q.discard("probe")





async def test_discard_reports_dropped():
    holder = {"memory": int(100), "disk": int(0)}
    q = Queues(maxsize=1024*4, watermarks=lambda: (int(holder["memory"]), int(holder["disk"])))
    for i in range(3): await q.publish({"m": i}, name="probe")
    dropped = await q.discard("probe")
    assert int(dropped) == 3





async def test_unnamed_bypasses_gate(q: Queues):
    for i in range(6): await q.publish({"i": i})
    assert [item["i"] for item in q.db] == [0, 1, 2, 3, 4, 5]





async def test_state_three_modes():
    holder = {"memory": int(0), "disk": int(0)}
    q = Queues(watermarks=lambda: (int(holder["memory"]), int(holder["disk"])))
    assert q.state() == "normal"
    holder["memory"] = 100
    assert q.state() == "soft"
    holder["memory"] = 0; holder["disk"] = 100
    assert q.state() == "hard"
