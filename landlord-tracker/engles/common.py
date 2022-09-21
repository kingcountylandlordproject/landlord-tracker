import yaml
from sqlalchemy import create_engine

CONFIG_PATH = "./config/postgres_db_config.yaml"

CREATE_USER_SQL = 'CREATE USER :username WITH PASSWORD :passwd'

def create_user_sql(username, passwd):
    return f'CREATE USER {username} WITH PASSWORD \'{passwd}\''

def create_database_sql(username, database):
    return f'CREATE DATABASE {database} WITH OWNER = {username};'

def drop_user_sql(username):
    return f'DROP USER {username}'

def drop_database_sql(database):
    return f'DROP DATABASE {database};'


def get_configs():
    with open(CONFIG_PATH, "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as exc:
            print(exc)
            exit()
    return config

def open_config_db(config):
    hostname = config['hostname']
    database_name = config['database_name']

    table_user = config['table_user']
    table_pw = config['table_pw']
    return create_engine(f'postgresql+psycopg2://{table_user}:{table_pw}@{hostname}/{database_name}')
