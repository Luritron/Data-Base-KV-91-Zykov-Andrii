import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URI = 'postgresql://postgres:qwerty@localhost:5432/Luritron'
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
base = declarative_base()
