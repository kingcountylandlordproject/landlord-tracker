import pandas as pd
from common import get_configs, get_db_configs, get_raw_data_configs, open_config_db
import sqlalchemy as db

def sum_column(engine, table_column):
    query = db.select([db.func.round(db.func.sum(table_column), 2)])
    sum_result = engine.execute(query).fetchall()
    return sum_result

def extract_land_value(db_config, raw_table_config):
    tables = raw_table_config['table_keys']

    engine = open_config_db(db_config)
    #with engine.connect() as conn:
    #    #conn.execute("commit")
    #    pass
    md = db.MetaData(bind=engine)

    #print(engine.table_names())
    table_config = tables['base_raw_tax']
    table_name = table_config['table_name']
    print(f"extracting total land value on: {table_name}")

    table = db.Table(table_name, md, autoload=True, autoload_with=engine)
    #sum_column(table.c.TaxableLandVal)
    land_val = sum_column(engine, table.c.ApprLandVal)[0][0]
    imp_val = sum_column(engine, table.c.ApprImpsVal)[0][0]
    print(land_val, imp_val)
    out = {'appraised_land_value_sum': land_val,
            'improvements_land_value_sum': imp_val,
            'land_val_pcnt': land_val / (land_val + imp_val),
            'imp_val_pcnt': imp_val / (land_val + imp_val),
            'total_val': land_val + imp_val
    }

    print(table.c)
    print(out)


def main():
    db_config = get_db_configs()
    raw_table_config = get_raw_data_configs()
    extract_land_value(db_config, raw_table_config)

if __name__ == '__main__':
    main()