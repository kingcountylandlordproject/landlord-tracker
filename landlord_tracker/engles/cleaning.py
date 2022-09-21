from common import open_config_db
from common import get_configs
import numpy as np
import pandas as pd
import os
import regex as re
# from word2number import w2n
import usaddress
# from scourgify import normalize_address_record
import sqlalchemy as db



ASSESSOR_PATH = './data/raw/EXTR_RPAcct_220706.csv'
LEGAL_PATH = './data/raw/property_legal_descriptions.csv'
RES_PATH = './data/raw/Residential Building/EXTR_ResBldg.csv'
APT_PATH = './data/raw/Apartment Complex/EXTR_AptComplex.csv'
CONDO_PATH = './data/raw/Condo Complex and Units/EXTR_CondoUnit2.csv'

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
        

def _owner_data():
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
    as_cols = ['Major', 'Minor', 'AcctNbr', 'TaxpayerName', 'AttnLine', 
               'AddrLine', 'CityState', 'ZipCode', 'ApprLandVal',
               'ApprImpsVal']
    asdf = pd.read_csv(ASSESSOR_PATH)[as_cols]
    
    # Get total appraised parcel value
    asdf['AppraisedVal'] = asdf['ApprLandVal'].fillna(0) + asdf['ApprImpsVal'].fillna(0)
    asdf = asdf.drop(columns=['ApprLandVal', 'ApprImpsVal'])
    
    # TO DO: Parse out owner addresses to match KC convention
    asdf = asdf.rename(columns={'TaxpayerName': 'OwnerName', 
                                'AttnLine': 'OwnerAttnLine',
                                'AddrLine': 'OwnerAddrLine',
                                'CityState': 'OwnerCityState',
                                'ZipCode': 'OwnerZipCode'})
    
    # Parse OwnerCityState into separate columns: City, State, Country
    
    return asdf

def _condos(owner_data):
    """
    Return dataframe of condo data
    """
    condo_cols = ['Major', 'Minor', 'UnitType', 'BldgNbr', 'UnitNbr', 
                  'Address', 'BuildingNumber', 'Fraction', 'DirectionPrefix', 
                  'StreetName', 'StreetType', 'DirectionSuffix', 'ZipCode']
    con_df = pd.read_csv(CONDO_PATH)[condo_cols]
    condos = con_df.merge(owner_data, on=['Major', 'Minor'], how='inner')
    return condos

def _rented_condos(owner_data, condos):
    # TODO: come up with metric to estimate which condos are rentals
    # Initial pass: estimate rentals where owner address doesn't contain
    # StreetName
    return

def _residential(owner_data):
    """
    Return dataframe of residential (townhouse and SFH) data
    """
    res_cols = ['Major', 'Minor', 'BldgNbr', 'Address', 'BuildingNumber', 
                'Fraction', 'DirectionPrefix', 'StreetName', 'StreetType', 
                'DirectionSuffix', 'ZipCode']
    res_df = pd.read_csv(RES_PATH, low_memory=False)[res_cols]
    return res_df.merge(owner_data, on=['Major', 'Minor'], how='inner')

def _apartments(owner_data):
    """
    Return dataframe of apartment building data
    """
    apt_cols = ['Major', 'Minor', 'ComplexDescr', 'NbrUnits', 'Address']
    apt_df = pd.read_csv(APT_PATH)[apt_cols]
    return apt_df.merge(owner_data, on=['Major', 'Minor'], how='inner')

def _get_ownership(df):
    n_props = df.pivot_table(index='OwnerName', aggfunc ='size').rename('NumProperties')
    val = df.pivot_table(values='AppraisedVal', index='OwnerName', aggfunc='sum')
    return pd.concat([n_props, val], axis=1).sort_values(by='NumProperties', ascending=False)


def _clean_data(config):
    tables = config['table_keys']

    engine = open_config_db(config)
    #with engine.connect() as conn:
    #    #conn.execute("commit")
    #    pass
    md = db.MetaData(bind=engine)

    #print(engine.table_names())
    table_config = tables['base_raw_tax']
    table_name = table_config['table_name']



    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    
    odf = _owner_data()

    # Load the Apartment, Condo, and Residential data
    condos = _condos(odf)
    resi = _residential(odf)
    apts = _apartments(odf)
    
    # Save apartments, condo, and residential data
    condos.to_csv(os.path.join(save_dir, 'condos.csv'))
    resi.to_csv(os.path.join(save_dir, 'residential.csv'))
    apts.to_csv(os.path.join(save_dir, 'apartments.csv'))
    return condos, resi, apts


def extract_land_value(config):
    tables = config['table_keys']

    engine = open_config_db(config)
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
    config = get_configs()
    _clean_data(config)