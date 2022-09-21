import pandas as pd

from sqlalchemy import create_engine 
from common import drop_database_sql, drop_user_sql, get_configs, create_user_sql, create_database_sql


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
        conn.execute("commit")
        try:
            conn.execute(drop_database_sql(database_name))
        except Exception as e:
            print(f"Warning, faluire to drop database {database_name}")
        try:
            conn.execute(drop_user_sql(table_user))
        except Exception as e:
            print(f"Warning, faluire to drop user {table_user}")
        conn.execute("commit")
        conn.execute(create_user_sql(table_user, table_pw))
        conn.execute("commit")
        conn.execute(create_database_sql(table_user, database_name))

    engine = create_engine(f'postgresql+psycopg2://{table_user}:{table_pw}@{hostname}/{database_name}')

    for key in tables.keys():
        print(f"uploading table key: {key}")
        table = tables[key]
        raw_path = table['path']
        table_name = table['table_name']
        df = pd.read_csv(f"{data_path}{raw_path}", encoding='latin-1')
        print(df.head())
        df.to_sql(table_name, engine)

def main():
    config = get_configs()
    create_databases_from_config(config)

if __name__ == '__main__':
    main()