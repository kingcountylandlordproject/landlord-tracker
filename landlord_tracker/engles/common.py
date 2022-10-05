import yaml
from sqlalchemy import create_engine
import os
from pyspark.sql import SparkSession

DB_CONFIG_PATH = "./config/postgres_db_config.yaml"
RAW_DATA_CONFIG_PATH = "./config/raw_data_config.yaml"
CLEANING_CONFIG_PATH = "./config/cleaning_config.yaml"

SPARK_CLASS_PATH = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.5.0 pyspark-shell'
CREATE_USER_SQL = 'CREATE USER :username WITH PASSWORD :passwd'
RAW_NAME = 'raw'
CLEAN_NAME = 'clean'

def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", SPARK_CLASS_PATH) \
    .getOrCreate()
    return spark

def get_spark_table(spark, config, table_type, table_name):
    return _get_spark_table(spark, config['table_user'], config['table_pw'], config['hostname'], config['db_names'][table_type], table_name)

def _get_spark_table(spark, table_user, table_pw, hostname, database_name, table_name):
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{hostname}/{database_name}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", table_user) \
        .option("password", table_pw) \
        .load()
    return df

#.option("url", f"jdbc:postgresql://{hostname}:5432/{database_name}") \

def save_spark_df(df, config, table_type, table_name):
    return _save_spark_df(df, config['table_user'], config['table_pw'], config['hostname'], config['db_names'][table_type], table_name)

def _save_spark_df(df, table_user, table_pw, hostname, database_name, table_name):
    df.write.format('jdbc').options(
        url= f'jdbc:postgresql://{hostname}/{database_name}',
        driver='org.postgresql.Driver',
        dbtable=table_name,
        user=table_user,
        password=table_pw).mode('overwrite').save()


def create_user_sql(username, passwd):
    return f'CREATE USER {username} WITH PASSWORD \'{passwd}\''

def create_database_sql(username, database):
    return f'CREATE DATABASE {database} WITH OWNER = {username};'

def drop_user_sql(username):
    return f'DROP USER {username}'

def drop_database_sql(database):
    return f'DROP DATABASE {database};'

def get_db_configs():
    with open(DB_CONFIG_PATH, "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as exc:
            print(exc)
            exit()
    return config


def get_raw_data_configs():
    with open(RAW_DATA_CONFIG_PATH, "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as exc:
            print(exc)
            exit()
    return config

def get_cleaning_configs():
    with open(CLEANING_CONFIG_PATH, "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as exc:
            print(exc)
            exit()
    return config

def open_config_db(config, name_type):
    hostname = config['hostname']
    database_name = config['db_names'][name_type]

    table_user = config['table_user']
    table_pw = config['table_pw']
    return create_engine(f'postgresql+psycopg2://{table_user}:{table_pw}@{hostname}/{database_name}')
