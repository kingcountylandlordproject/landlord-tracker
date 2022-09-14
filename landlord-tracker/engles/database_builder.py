import pandas as pd
import yaml

from sqlalchemy import create_engine 

CONFIG_PATH = "./config/postgres_db_config.yaml"


CREATE_USER_SQL = 'CREATE USER :username WITH PASSWORD :passwd'

def create_user_sql(username, passwd):
    return f'CREATE USER {username} WITH PASSWORD \'{passwd}\''

def create_database_sql(username, database):
    return f'CREATE DATABASE {database} WITH OWNER = {username};'


def get_configs():
    with open(CONFIG_PATH, "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as exc:
            print(exc)
            exit()
    return config

def create_databases_from_config(config):
    user = config['user']
    user_pw = config['user_pw']
    hostname = config['hostname']
    data_path = config['data_path']
    database_name = config['database_name']
    #encryption_pw = config['encryption_pw']
    tables = config['table_keys']

    table_user = config['table_user']
    table_pw = config['table_pw']
    engine = create_engine(f'postgresql+psycopg2://{user}:{user_pw}@{hostname}')
    
    with engine.connect() as conn:
        conn.execute(create_user_sql(table_user, table_pw))
        conn.execute("commit")
        conn.execute(create_database_sql(table_user, database_name))

    engine = create_engine(f'postgresql+psycopg2://{table_user}:{table_pw}@{hostname}/{database_name}')

    for key in tables.keys():
        print(f"uploading table key: {key}")
        table = tables[key]
        raw_path = table['raw_path']
        df = pd.read_csv(f"{data_path}{raw_path}", encoding='latin-1')
        print(df.head())
        df.to_sql(key, engine)

def main():
    config = get_configs()
    create_databases_from_config(config)

if __name__ == '__main__':
    main()