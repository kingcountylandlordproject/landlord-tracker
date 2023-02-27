import os

from sqlalchemy import create_engine


def get_db_engine(user=None, password=None, hostname=None):
    user = user or os.getenv('DB_USER')
    password = password or os.getenv('DB_PASSWORD')
    hostname = hostname or os.getenv('DB_HOSTNAME')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}')
    return engine
