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


async def get_element2(url, session):
    async with session.get(url) as response:
        return await response.json()


def url_list(peoples, odj_key):
    set_url = set()
    for people in peoples:
        if people.get('detail') == 'Not found':
            continue
        for key_urs in odj_key:
            for url in people.get(key_urs):
                if global_cash.get(url) is None:
                    set_url.add(url)
        url = people.get('homeworld')
        if global_cash.get(url) is None:
            set_url.add(url)
    return set_url


def get_key(context):
    if context == 'films':
        return 'title'
    if context == 'planets' or context == 'species':
        return 'name'
    return 'model'


async def add_in_cash(people, obj_key):
    urls = url_list(people, obj_key)
    session = ClientSession()
    coros = (get_element2(i, session) for i in urls)
    results = await asyncio.gather(*coros)
    await session.close()
    # доб в кеш
    for res in results:
        url = res.get('url')
        context = url.split('/')[4]
        key = get_key(context)
        global_cash[url] = res.get(key)
    print(len(global_cash))


def get_str(urls):
    return ', '.join([global_cash[url] for url in urls])


async def insert_people(people_chunk):
    async with Session() as session:
        object_key = ['films', 'species', 'starships', 'vehicles']
        await add_in_cash(people_chunk, object_key)
        for people in people_chunk:
            if people.get('detail') != 'Not found':
                film_list = get_str(people.get('films'))
                home_world_list = get_str([people.get('homeworld')])
                species_list = get_str(people.get('species'))
                star_ships_list = get_str(people.get('starships'))
                venicals_list = get_str(people.get('vehicles'))

                session.add_all([SwPeople(
                    # json=people,
                    id=int(people.get('url').split('/')[5]),
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
# print(global_cash)
