import csv
from enum import Enum, auto
import os
import re
import tempfile

from sqlalchemy import create_engine

from .common import get_data_path, get_db_data_path, get_load_manifest

class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name
    
class FileFormat(AutoName):
    CSV = auto()
    TSV = auto()

    @classmethod
    def parse(cls, format_str):
        return cls(format_str.upper())


def get_db_engine(user=None, password=None, hostname=None):
    user = user or os.getenv('DB_USER')
    password = password or os.getenv('DB_PASSWORD')
    hostname = hostname or os.getenv('DB_HOSTNAME')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}')
    return engine

CAMEL_TO_SNAKE_RE = re.compile(r'(?<!^)(?=[A-Z])')

def camel_to_snake(s):
    return CAMEL_TO_SNAKE_RE.sub('_', s).lower()

def get_column_names_header(path, format: FileFormat=None):
    if format == FileFormat.CSV:
        with open(path, newline='') as f:
            reader = csv.reader(f)
            column_names = next(reader)
    elif format == FileFormat.TSV:
        with open(path) as f:
            firstline = f.readline()
            column_names = firstline.split("\t")
    else:
        raise Exception("unknown format or format not supported")
    return [ camel_to_snake(col) for col in column_names ]


def clean(val):
    return val.replace("\n", "")


def do_remove_newlines_in_values(source, target, encoding='utf-8'):
    with open(target, "w", encoding=encoding) as output:
        writer = csv.writer(output)
        with open(source, encoding=encoding) as input:
            reader = csv.reader(input)
            for line in reader:
                cleaned = [clean(val) for val in line]
                writer.writerow(cleaned)


def load_all():
    engine = get_db_engine()

    load_manifest = get_load_manifest()
        
    tables = load_manifest['tables']

    with engine.connect() as conn:
        for table in tables.keys():
            with conn.begin():
                table_entry = tables[table]

                full_path = os.path.join(get_data_path(), table_entry['path'])
                format = FileFormat.parse(table_entry.get('format', 'csv'))
                encoding = table_entry.get('encoding', 'utf-8')
                load = table_entry.get('load', 'full')
                remove_newlines_in_values = table_entry.get('remove_newlines_in_values', False)

                print(f"Loading table: {table}")

                if remove_newlines_in_values:
                    print("Removing newlines in file")
                    tmpfile = tempfile.NamedTemporaryFile() 
                    do_remove_newlines_in_values(full_path, tmpfile.name, encoding=encoding)
                    full_path = tmpfile.name
                
                if load == 'full':
                    drop_table_sql = f"DROP TABLE IF EXISTS {table}"
                    conn.execute(drop_table_sql)

                headers = get_column_names_header(full_path, format)
                fields = ",".join([ f"{header} VARCHAR" for header in headers ])
                
                create_table_sql = f"CREATE TABLE {table} ({fields})"
                conn.execute(create_table_sql)

                # path which the server (not this process) uses
                server_path = get_db_data_path(table_entry['path'])

                sql_copy = f"COPY {table} ({','.join(headers)}) FROM '{server_path}' WITH (FORMAT csv, HEADER, ENCODING '{encoding}', FORCE_NULL({','.join(headers)}))"
                if format == FileFormat.TSV:
                    sql_copy += " DELIMITER '\t'"

                conn.execute(sql_copy)

                cursor = conn.execute(f"SELECT COUNT(*) as num_rows FROM {table}")

                print(f"Loaded {cursor.fetchone()[0]} rows")

                if remove_newlines_in_values:
                    os.unlink(tmpfile.name)
