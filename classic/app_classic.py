import requests
import re
from db_classic import Session, SwPeople
from datetime import datetime

global_cash = {}


def list_people(url='https://swapi.dev/api/people'):
    # params = {'user_ids': self.id}
    response = requests.get(url)
    return response.json()


def get_films(urls_films: list):
    films_list = []
    for url in urls_films:
        if global_cash.get(url) is None:
            response = requests.get(url)
            global_cash[url] = response.json()["title"]
        films_list.append(global_cash[url])
    return films_list


def get_species(urls_species: list):
    if len(urls_species) == 0:
        return []
    else:
        species_list = []
        for url in urls_species:
            if global_cash.get(url) is None:
                response = requests.get(url)
                global_cash[url] = response.json()["name"]
            species_list.append(global_cash[url])
        return species_list


def get_star_ships(urls_ships: list):
    if len(urls_ships) == 0:
        return []
    else:
        ships_list = []
        for url in urls_ships:
            if global_cash.get(url) is None:
                response = requests.get(url)
                global_cash[url] = response.json()["model"]
            ships_list.append(global_cash[url])
        return ships_list


def get_vehicles(urls_vehicles: list):
    if len(urls_vehicles) == 0:
        return []
    else:
        vehicles_list = []
        for url in urls_vehicles:
            if global_cash.get(url) is None:
                response = requests.get(url)
                global_cash[url] = response.json()["model"]
            vehicles_list.append(global_cash[url])
        return vehicles_list


def get_homeworld(urls_homeworld):
    if global_cash.get(urls_homeworld) is None:
        response = requests.get(urls_homeworld)
        global_cash[urls_homeworld] = response.json()["name"]
    return global_cash.get(urls_homeworld)


def insert_in_db(url='https://swapi.dev/api/people'):
    peopls = list_people(url)
    for people in (peopls['results']):
        id = re.search(r'\d+', people['url']).group(0)
        people['id'] = int(id)
        # print (people['id'])
        with Session() as session:
            new_people = SwPeople(
                id=people.get('id'),
                name=people.get('name'),
                birth_year=people.get('birth_year'),
                eye_color=people.get('eye_color'),
                # films=','.join(people.get('films')),
                films=','.join(get_films(people.get('films'))),
                gender=people.get('gender'),
                hair_color=people.get('hair_color'),
                height=people.get('height'),
                homeworld=get_homeworld(people.get('homeworld')),
                mass=people.get('mass'),
                skin_color=people.get('skin_color'),
                species=','.join(get_species(people.get('species'))),
                starships=','.join(get_star_ships(people.get('starships'))),
                vehicles=','.join(get_vehicles(people.get('vehicles')))

            )
            session.add(new_people)
            session.commit()
    if peopls['next'] is not None:
        insert_in_db(url=peopls['next'])


start = datetime.now()
(insert_in_db())
print(datetime.now() - start)
