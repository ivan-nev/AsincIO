import asyncio
import datetime
from aiohttp import ClientSession
from IO.db_io import SwPeople, engine, Base, Session
from more_itertools import chunked



CHUNK_SIZE = 10


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
    print(f'end {people_id}')
    return json_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 90), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        for people in people_chunk:
            if people.get('detail') != 'Not found':
                # print(people)
                session.add_all([SwPeople(
                    # json=people,
                    name=people.get('name'),
                    birth_year=people.get('birth_year'),
                    eye_color=people.get('eye_color'),
                    films=','.join(people.get('films')),
                    gender=people.get('gender'),
                    hair_color=people.get('hair_color'),
                    height=people.get('height'),
                    homeworld=people.get('homeworld'),
                    mass=people.get('mass'),
                    skin_color=people.get('skin_color'),
                    species=','.join(people.get('species')),
                    starships=','.join(people.get('starships')),
                    vehicles=','.join(people.get('vehicles'))
                                )
                                 ])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
