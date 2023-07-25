import asyncio

from amongo.connection import Connection


async def create_people(conn: Connection) -> None:
    people = [{"name": "John", "age": age} for age in range(100)]

    await conn.coll("people").insert_many(people)


async def main() -> None:
    conn = Connection("mongodb://localhost:27017/data")
    await conn.open()

    await create_people(conn)

    cursor = await conn.coll("people").find(
        {"name": "John"},
    )

    async for doc in cursor:
        print(doc)


asyncio.run(main())
