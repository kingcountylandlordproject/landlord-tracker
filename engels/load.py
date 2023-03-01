import csv
from enum import Enum, auto
import io
import os
import re
import subprocess
import tempfile

from .common import get_data_path, get_db_data_path, get_load_manifest, get_project_path, readable_timestamp
from .db import get_db_engine

class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

class FileFormat(AutoName):
    CSV = auto()
    TSV = auto()

    @classmethod
    def parse(cls, format_str):
        return cls(format_str.upper())


CAMEL_TO_SNAKE_RE = re.compile(r'(?<!^)(?=[A-Z])')

def camel_to_snake(s):
    if s == s.upper():
        return s.lower()
    s = s.replace(" ", "_")
    snake = CAMEL_TO_SNAKE_RE.sub('_', s)
    return re.sub("_{2,}", "_", snake).lower()


def get_first_line(path):
    with open(path) as f:
        return f.readline()


def parse_header(line: str, format: FileFormat=None, normalize=False):
    """
    parse the header string (first line of a file) containing column names
    """
    if format == FileFormat.CSV:
        f = io.StringIO(line)
        reader = csv.reader(f)
        column_names = next(reader)
    elif format == FileFormat.TSV:
        column_names = [col.strip() for col in line.split("\t")]
    else:
        raise Exception("unknown format or format not supported")

    if normalize:
        return [ camel_to_snake(col) for col in column_names ]
    else:
        return column_names


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

                first_line = get_first_line(full_path)
                original_column_names = parse_header(first_line, format)
                column_names = parse_header(first_line, format, normalize=True)
                fields = ",".join([ f"{header} VARCHAR" for header in column_names ])

                create_table_sql = f"CREATE TABLE {table} ({fields})"
                conn.execute(create_table_sql)

                # path which the server (not this process) uses
                server_path = get_db_data_path(table_entry['path'])

                if format == FileFormat.TSV:
                    sql_copy = f"COPY {table} ({','.join(column_names)}) FROM '{server_path}' WITH (FORMAT text, ENCODING '{encoding}')"
                else:
                    sql_copy = f"COPY {table} ({','.join(column_names)}) FROM '{server_path}' WITH (FORMAT csv, HEADER, ENCODING '{encoding}', FORCE_NULL({','.join(column_names)}))"

                conn.execute(sql_copy)

                # the 'header' option in COPY is only available for csv, so delete the row with the column names
                if format == FileFormat.TSV:
                    where = " AND ".join([f"\"{col}\" = %s" for col in column_names])
                    conn.execute(f"DELETE FROM {table} WHERE {where}", original_column_names)

                cursor = conn.execute(f"SELECT COUNT(*) as num_rows FROM {table}")

                print(f"Loaded {cursor.fetchone()[0]} rows")

                if remove_newlines_in_values:
                    os.unlink(tmpfile.name)


def create_data_package(all=False, include_preprocessed=False):
    """
    Create a .zip file containing all the files under the data/
    directory needed to satisfy the load manifest file
    """
    data_dir = os.path.join(get_project_path(), "data")

    load_manifest = get_load_manifest()

    tables = load_manifest['tables']

    if all:
        target = f"data_all_{readable_timestamp()}.zip"
        command = f"zip {target} -r preprocessed raw"
    else:
        paths = []
        for table in tables.keys():
            table_entry = tables[table]
            include = True
            if not include_preprocessed:
                include = not bool(table_entry.get('preprocess', False))
            if include:
                paths.append(table_entry['path'])

        target = f"data_minimal_{readable_timestamp()}.zip"

        command = f"zip {target} {' '.join(paths)}"

    print(f"Creating {target}")
    subprocess.run(command, cwd=data_dir, shell=True)
