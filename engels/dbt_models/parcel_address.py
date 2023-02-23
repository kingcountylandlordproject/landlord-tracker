from sqlalchemy import select
from sqlalchemy.orm import Session
from scourgify import normalize_address_record

from ..db import get_db_engine


# not sure these addresses are useful to normalize: probably
# more useful to do tax payer addresses in real_property_account
def create_parcel_address():
    engine = get_db_engine()

    with engine.connect() as conn:
        conn.execute(
            """
        DROP TABLE IF EXISTS parcel_address
        """
        )

        conn.execute(
            """
        CREATE TABLE parcel_address (
            major_minor VARCHAR
            ,address VARCHAR
            ,address_normalized VARCHAR
            ,address_zipcode_normalized VARCHAR
            )
        """
        )

        cursor = conn.execute(
            "SELECT major_minor, address FROM int_parcel_address"
        ).mappings()

        while True:
            insert_batch = []
            rows = cursor.fetchmany(1000)
            if rows:
                for row in rows:
                    normalized_dict = None
                    if row["address"]:
                        try:
                            normalized_dict = normalize_address_record(row["address"])
                        except:
                            print(f"Could not normalize: {row['address']}")

                    if normalized_dict:
                        address_normalized = " ".join(
                            [
                                line
                                for line in [
                                    normalized_dict["address_line_1"],
                                    normalized_dict["address_line_2"],
                                ]
                                if line
                            ]
                        )
                        insert_batch.append(
                            [
                                row["major_minor"],
                                row["address"],
                                address_normalized,
                                normalized_dict["postal_code"],
                            ]
                        )
                # insert
                dbapi_conn = conn.connection
                insert_cursor = dbapi_conn.cursor()
                insert_query = "INSERT INTO parcel_address(major_minor,address,address_normalized,address_zipcode_normalized) VALUES(%s, %s, %s, %s)"
                insert_cursor.executemany(insert_query, insert_batch)
                dbapi_conn.commit()
            else:
                break


if __name__ == "__main__":
    create_parcel_address()
