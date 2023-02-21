import yaml
from sqlalchemy import create_engine
import os

LOAD_MANIFEST_PATH = "config/load_manifest.yml"

def get_project_path():
    # dir = os.path.dirname(__file__)
    # # ugh
    # project_path = os.path.abspath(os.path.join(dir, "../.."))
    # print(f"project_path={project_path}")
    # return project_path
    return os.getenv('PROJECT_PATH') or os.getcwd()


def get_data_path():
    return os.path.join(get_project_path(), "data")


def get_db_data_path(path):
    """
    returns path to data files, as accessed by postgres
    """
    return os.path.join("/data", path)


def create_spark_session():
    spark = SparkSession \
    .builder \
    .getOrCreate()
    return spark

def get_spark_table(spark, config, table_type, table_name):
    return _get_spark_table(spark, config['user'], config['user_pw'], config['hostname'], config['user'], table_name)

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
    return _save_spark_df(df, config['user'], config['user_pw'], config['hostname'], config['user'], table_name)

def _save_spark_df(df, table_user, table_pw, hostname, database_name, table_name):
    df.write.format('jdbc').options(
        url= f'jdbc:postgresql://{hostname}/{database_name}',
        driver='org.postgresql.Driver',
        dbtable=table_name,
        user=table_user,
        password=table_pw).mode('overwrite').save()

def get_load_manifest():
    path = os.path.join(get_project_path(), LOAD_MANIFEST_PATH)
    with open(path, "r") as config:
        try:
            config = yaml.safe_load(config)
        except yaml.YAMLError as exc:
            print(exc)
            exit()
    return config
