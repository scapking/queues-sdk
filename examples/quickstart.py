import asyncio

from queues import Queues


async def main():
    q = Queues()

    await q.publish({"event": "signup", "user": 1})
    message = await q.consume()
    print("unnamed round-trip:", message)

    await q.publish({"event": "audit", "user": 2}, name="audit")
    message = await q.consume(name="audit")
    print("named round-trip:", message)

    print("pressure state:", q.state())


if __name__ == "__main__":
    asyncio.run(main())
