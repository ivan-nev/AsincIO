import asyncio
import datetime
from aiohttp import ClientSession
from IO.db_io import SwPeople, engine, Base, Session
from more_itertools import chunked

global_cash = {}

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


async def get_element(url, session):
    async with session.get(url) as response:
        return await response.json()


async def get_elements(urls: list, key):
    # Проверка в кеше, если нет в кеше, доб в  список запросов
    new_url_list = []
    for url in urls:
        if global_cash.get(url) is None:
            new_url_list.append(url)
    if len(new_url_list) > 0:
        session = ClientSession()
        coros = (get_element(i, session) for i in new_url_list)
        results = await asyncio.gather(*coros)
        await session.close()
        # доб в кеш
        for res in results:
            global_cash[res['url']] = res[key]
        # print (len(global_cash))
    return ', '.join([global_cash[u] for u in urls])


async def insert_people(people_chunk):
    async with Session() as session:
        # async with ClientSession() as session_in:
        for people in people_chunk:
            if people.get('detail') != 'Not found':
                film_list = await get_elements(people.get('films'), 'title')
                home_world_list = await get_elements([people.get('homeworld')], 'name')
                species_list = await get_elements(people.get('species'), 'name')
                star_ships_list = await get_elements(people.get('starships'), 'model')
                venicals_list = await get_elements(people.get('vehicles'), 'model')
                id = people.get('url').split('/')[5]

                session.add_all([SwPeople(
                    # json=people,
                    id=int(id),
                    name=people.get('name'),
                    birth_year=people.get('birth_year'),
                    eye_color=people.get('eye_color'),
                    films=film_list,
                    gender=people.get('gender'),
                    hair_color=people.get('hair_color'),
                    height=people.get('height'),
                    homeworld=home_world_list,
                    mass=people.get('mass'),
                    skin_color=people.get('skin_color'),
                    species=species_list,
                    starships=star_ships_list,
                    vehicles=venicals_list
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
print(global_cash)
