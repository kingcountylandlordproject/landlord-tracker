import pandas as pd

from sqlalchemy import create_engine 
from common import CLEAN_NAME, RAW_NAME, drop_database_sql, drop_user_sql, get_db_configs, get_raw_data_configs, create_user_sql, create_database_sql, open_config_db


def create_databases_from_config(db_config, raw_table_config):
    user = db_config['user']
    user_pw = db_config['user_pw']
    hostname = db_config['hostname']
    data_path = db_config['data_path']
    database_name_raw = db_config['db_names'][RAW_NAME]
    database_name_clean = db_config['db_names'][CLEAN_NAME]


    table_user = db_config['table_user']
    table_pw = db_config['table_pw']
    engine = create_engine(f'postgresql+psycopg2://{user}:{user_pw}@{hostname}')
    
    # run these statements in a connection with autocommit to avoid these errors:
    # "ERROR:  DROP DATABASE [or other statement] cannot run inside a transaction block"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        try:
            conn.execute(drop_database_sql(database_name_raw))
        except Exception as e:
            print(f"Warning, faluire to drop database {database_name_raw}")
        try:
            conn.execute(drop_database_sql(database_name_clean))
        except Exception as e:
            print(f"Warning, faluire to drop database {database_name_clean}")
        try:
            conn.execute(drop_user_sql(table_user))
        except Exception as e:
            print(f"Warning, faluire to drop user {table_user}")
        conn.execute(create_user_sql(table_user, table_pw))
        conn.execute(create_database_sql(table_user, database_name_raw))
        conn.execute(create_database_sql(table_user, database_name_clean))


    engine = open_config_db(db_config, RAW_NAME)

    tables = raw_table_config['table_keys']
    for key in tables.keys():
        print(f"uploading table key: {key}")
        table = tables[key]
        raw_path = table['path']
        table_name = table['table_name']
        df = pd.read_csv(f"{data_path}{raw_path}", encoding='latin-1')
        print(f'sending to sql {table_name} on engine {engine}\n')
        print(df.head())
        df.to_sql(table_name, engine)

def main():
    db_config = get_db_configs()
    raw_table_config = get_raw_data_configs()
    create_databases_from_config(db_config, raw_table_config)

if __name__ == '__main__':
    main()