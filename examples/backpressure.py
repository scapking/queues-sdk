import asyncio

from queues import Queues


async def main():
    q = Queues(maxsize=8)                        # small buffer on purpose

    async def producer(count):
        for i in range(count):
            await q.publish({"i": i})            # blocks when full, never drops

    async def consumer(count):
        for i in range(count):
            print("consumed:", (await q.consume())["i"])

    await asyncio.gather(producer(10), consumer(5))
    await asyncio.sleep(0.05)
    await consumer(5)                            # drains the rest


if __name__ == "__main__":
    asyncio.run(main())
