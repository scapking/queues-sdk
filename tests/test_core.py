import asyncio

import pytest

from queues_sdk import Queues, QueuesError





async def test_fifo_order(q: Queues):
    for i in range(6): await q.publish({"i": i})
    assert [await q.consume() for _ in range(6)] == [{"i": i} for i in range(6)]





async def test_waiter_handoff(q: Queues):
    seen = []
    async def consumer():
        while True: seen.append(await q.consume())
    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.01)
    for i in range(6):
        await q.publish({"i": i})
        await asyncio.sleep(0.01)
    assert [item["i"] for item in seen] == [0, 1, 2, 3, 4, 5]
    task.cancel()
    await asyncio.sleep(0)





async def test_full_backpressure_zero_loss(q: Queues):
    capacity = int(q.db.maxlen)
    for i in range(capacity): await q.publish({"i": i})
    tasks = [asyncio.create_task(q.publish({"i": capacity + j})) for j in range(3)]
    await asyncio.sleep(0.05)
    assert all(not task.done() for task in tasks)
    for _ in range(3): await q.consume()
    await asyncio.sleep(0.05)
    assert all(task.done() for task in tasks)
    assert len(q.db) == capacity
    assert int(q.db[0]["i"]) == 3 and int(q.db[-1]["i"]) == capacity + 2
    q.db.clear()





async def test_consume_cancel_cleanup(q: Queues):
    task = asyncio.create_task(q.consume())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert len(q.waiters) == 0
    await q.publish({"i": 1})
    assert (await q.consume())["i"] == 1


async def test_backpressure_pipeline_fifo():
    # 死锁回归：生产者腾位后若不看 waiter 直接落 db，最后一条消息永久卡死消费者。
    q = Queues(maxsize=1, watermarks=lambda: (int(0), int(0)))
    received = []
    async def producer():
        for i in range(200): await q.publish({"i": i})
    async def consumer():
        for _ in range(200): received.append(await q.consume())
    await asyncio.gather(producer(), consumer())
    assert [item["i"] for item in received] == list(range(200))





async def test_named_error_type():
    error = QueuesError("probe")
    assert str(error) == "probe"
