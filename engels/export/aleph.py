# export files for import into Aleph
#
from engels.common import get_db_data_path
from engels.db import get_db_engine


def export_all():
    engine = get_db_engine()
    with engine.connect() as conn:
        with conn.begin():
            path = get_db_data_path("export/aleph_real_property_account.csv")
            sql = f"COPY aleph_real_property_account TO '{path}' WITH DELIMITER ',' CSV HEADER"
            conn.execute(sql)

            path = get_db_data_path("export/aleph_apartment_complex.csv")
            sql = f"COPY stg_apartment_complex TO '{path}' WITH DELIMITER ',' CSV HEADER"
            conn.execute(sql)

if __name__ == "__main__":
    print("Exporting tables")
    export_all()