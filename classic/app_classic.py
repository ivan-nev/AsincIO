import requests
import re
from db_classic import Session, SwPeople
from datetime import datetime


def list_people(url='https://swapi.dev/api/people'):
    # params = {'user_ids': self.id}
    response = requests.get(url)
    return response.json()


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
            session.add(new_people)
            session.commit()
    if peopls['next'] is not None:
        insert_in_db(url=peopls['next'])


start = datetime.now()
(insert_in_db())
print(datetime.now() - start)
