from common import RAW_NAME, SPARK_CLASS_PATH, create_spark_session, get_cleaning_configs, get_db_configs, get_raw_data_configs, get_spark_table, save_spark_df
import numpy as np
import pandas as pd
import os
import regex as re
# from word2number import w2n
import usaddress
# from scourgify import normalize_address_record
import sqlalchemy as db

from pyspark.sql.functions import col

CONDO_TABLE_TYPE = 'condo_raw'
RESIDENTIAL_TABLE_TYPE = 'residential_raw'
APARTMENT_TABLE_TYPE = 'apartment_raw'

CONDO_OUT = 'condo_clean'
RESIDENTIAL_OUT = 'residential_clean'
APARTMENT_OUT = 'apartment_clean'

CONDO_COLS = ['Major', 'Minor', 'UnitType', 'BldgNbr', 'UnitNbr', 
                'Address', 'BuildingNumber', 'Fraction', 'DirectionPrefix', 
                'StreetName', 'StreetType', 'DirectionSuffix', 'ZipCode']
RESIDENTIAL_COLS = ['Major', 'Minor', 'BldgNbr', 'Address', 'BuildingNumber', 
            'Fraction', 'DirectionPrefix', 'StreetName', 'StreetType', 
            'DirectionSuffix', 'ZipCode']
APARTMENT_COLS = ['Major', 'Minor', 'ComplexDescr', 'NbrUnits', 'Address']

CLEAN_LIST = [
        {
        'cols': CONDO_COLS,
        'table_type': CONDO_TABLE_TYPE,
        'out_type': CONDO_OUT,
        },
        {
        'cols': RESIDENTIAL_COLS,
        'table_type': RESIDENTIAL_TABLE_TYPE,
        'out_type': RESIDENTIAL_OUT,
        },
        {
        'cols': APARTMENT_COLS,
        'table_type': APARTMENT_TABLE_TYPE,
        'out_type': APARTMENT_OUT,
        }
    ]

def _check_pobox(addr: str):
    return re.sub(r'[^\w\s]', '', addr.split(' ')[0]).upper().startswith('P')

def _parse_addr_line(addr: str):
    # TO DO: look for 
    if not _check_pobox(str(addr)):
        try:
            return usaddress.tag(addr)
        except:
            return # Hand parse? Fails on foreign addressess
        
def _parse_city_state(city_state: str):
    """
    TODO: Fix this, as it's not functioning properly
    """
    # Remove punctuation and trailing or leading white space
    city_state = re.sub(r'[^\w\s\d-]', '', city_state.rstrip().lstrip())
    # Separate out state
    match = re.search('(?P<city>.*?)\s+(?P<state>[A-Z]{2}$)', city_state)
    if match:
        return match['city'], match['state']
    else:
        return '', ''
        

def _owner_data(spark, db_config, raw_tables):
    """
    Load assessor data, and return DataFrame with relevant info: 
    -AcctNbr -- Parcel Tax ID
    -TaxpayerName -- Parcel owner name
    -AttnLine -- Some info on owner name
    -AddrLine -- Parcel owner address
    -CityState -- City and State of parcel owner
    -ZipCode -- zip code of parcel owner
    -ApprLandVal -- appraised land value 
    -ApprImpsVal -- appraised improvements (building) values
    -TaxableLandVal -- taxable land value
    -TaxableImpsVal -- table improvements value
    """
    owner_cols = ['Major', 'Minor', 'AcctNbr', 'TaxpayerName', 'AttnLine', 
               'AddrLine', 'CityState', 'ZipCode', 'ApprLandVal',
               'ApprImpsVal']
    table_name = raw_tables['base_raw_tax']['table_name']
    #pdf = pd.read_sql(f'select * from {table_name}', engine)[as_cols]
    pdf = get_spark_table(spark, db_config, RAW_NAME, table_name)[owner_cols]
    
    
    #spark.createDataFrame(pdf)

    # Get total appraised parcel value
    pdf.na.fill(value=0,subset=['ApprLandVal','ApprImpsVal'])
    pdf = pdf.withColumn("AppraisedVal", col("ApprLandVal") + col("ApprImpsVal"))

    #pdf = pdf.drop(columns=['ApprLandVal', 'ApprImpsVal'])
    rename_columns = {
                        'TaxpayerName': 'OwnerName', 
                        'AttnLine': 'OwnerAttnLine',
                        'AddrLine': 'OwnerAddrLine',
                        'CityState': 'OwnerCityState',
                        'ZipCode': 'OwnerZipCode'
                    }    

    for key, value in rename_columns.items():
        pdf.withColumnRenamed(key, value)
    
    #TODO: Parse out owner addresses to match KC convention    
    #TODO: Parse OwnerCityState into separate columns: City, State, Country
    return pdf


def _clean_table_type(owner_data, spark, db_config, raw_tables, table_type, cols, out_type):
    table_name = raw_tables[table_type]['table_name']
    df = get_spark_table(spark, db_config, RAW_NAME, table_name)[cols]
    condos = df.merge(owner_data, on=['Major', 'Minor'], how='inner')
    return condos

def _rented_condos(owner_data, condos):
    # TODO: come up with metric to estimate which condos are rentals
    # Initial pass: estimate rentals where owner address doesn't contain
    # StreetName
    return


def _get_ownership(df):
    n_props = df.pivot_table(index='OwnerName', aggfunc ='size').rename('NumProperties')
    val = df.pivot_table(values='AppraisedVal', index='OwnerName', aggfunc='sum')
    return pd.concat([n_props, val], axis=1).sort_values(by='NumProperties', ascending=False)


def _clean_data():

    db_config = get_db_configs()
    raw_table_config = get_raw_data_configs()
    clean_table_config = get_cleaning_configs()

    _clean_data_helper(db_config, raw_table_config, clean_table_config)


def _clean_data_helper(db_config, raw_table_config, clean_table_config):
    tables = raw_table_config['table_keys']
    spark = create_spark_session()
    in_configs = {'spark': spark,
        'db_config': db_config,
        'raw_tables': tables}

    data_path = db_config['data_path']
    odf = _owner_data(**in_configs)


    clean_tables = [{**in_configs,**cl} for cl in CLEAN_LIST]
    clean_tables = {ct['out_type']: _clean_table_type(odf, **ct) for ct in clean_tables}

    for key, clean_table in clean_tables.items():
        clean_table.to_csv(os.path.join(data_path, clean_table_config[key]['path']))
        save_spark_df(clean_table, in_configs, clean_table_config[key]['table_name'])


def main():
    _clean_data()

if __name__ == '__main__':
    main()