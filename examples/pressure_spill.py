import asyncio

from queues import Queues


async def main():
    holder = {"memory": 0, "disk": 0}
    q = Queues(maxsize=1024*4, watermarks=lambda: (int(holder["memory"]), int(holder["disk"])))

    holder["memory"] = 100                       # cross the memory soft line
    for i in range(5):
        await q.publish({"m": i}, name="audit")  # buffered into pending
    print("soaked in pending:", sum(len(v) for v in q.pending.values()))

    holder["memory"] = 0                         # pressure recovers
    drained = []
    pulled = await q.drain("audit", drained)
    print("drained:", pulled, "in FIFO order:", [x["m"] for x in drained])

    dropped = await q.discard("audit")
    print("dropped after drain:", dropped)


if __name__ == "__main__":
    asyncio.run(main())
