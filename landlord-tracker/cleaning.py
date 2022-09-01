import numpy as np
import pandas as pd
import os
import regex as re
from word2number import w2n
import usaddress
from scourgify import normalize_address_record


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
    #asdf['Address'] = asdf['AddrLine'] + ' ' + asdf['CityState'] + ' ' + asdf['ZipCode'].astype(str)
    #addrs = asdf['Address'].apply(_parse_addr_line) 
    #addrs = pd.DataFrame(asdf['Address'].apply(_parse_addr_line))
    asdf = asdf.rename(columns={'TaxpayerName': 'OwnerName', 
                                'AttnLine': 'OwnerAttnLine',
                                'AddrLine': 'OwnerAddrLine',
                                'CityState': 'OwnerCityState',
                                'ZipCode': 'OwnerZipCode'})
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
    res_df = pd.read_csv(RES_PATH)[res_cols]
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

def _clean_data(save_dir):
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    
    odf = _owner_data()

    # Load the parcel legal descriptions, also from King County Assessor
    #ldf = pd.read_csv(LEGAL_PATH)
    #ldf = ldf[['account_number', 'parcel_number', 'legal_description']].set_index('account_number')
    
    # Load the Apartment, Condo, and Residential data
    condos = _condos(odf).to_csv(os.path.join(save_dir, 'condos.csv'))
    resi = _residential(odf).to_csv(os.path.join(save_dir, 'residential.csv'))
    apts = _apartmetns(odf).to_csv(os.path.join(save_dir, 'residential.csv'))
    return

_clean_data(save_dir='./data/clean')