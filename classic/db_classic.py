import atexit
from sqlalchemy import Column, String, Integer, DateTime, create_engine, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from classic.config_classic import PG_DSN




# PG_DSN = 'postgresql://app:123@0.0.0.0:5432/netology'
engine = create_engine(PG_DSN)

Base = declarative_base()
Session = sessionmaker(bind=engine)
#закрытие сесии после завершения программы
atexit.register(engine.dispose)

# создаём таблицы


class SwPeople(Base):

    __tablename__ = 'swpeople'

    id = Column(Integer, primary_key=True,)
    name = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

