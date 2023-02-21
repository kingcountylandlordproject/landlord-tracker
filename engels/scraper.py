#!/usr/bin/env python
"""
Tools for scraping data from the King County API
"""

import pandas as pd
from sodapy import Socrata
from arcgis.gis import GIS
import os
import requests
from bs4 import BeautifulSoup

EREAL_URL = 'https://blue.kingcounty.com/Assessor/eRealProperty/Detail.aspx'
GIS_GEOJSON_URL = 'https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/property__parcel_address_area/MapServer/1722/query?outFields=*&where=1%3D1&f=geojson'
DATAPATH = './data'

def get_legal_descriptions():
    """
    Pulls the latest dataset results from the King County Property Legal 
    Descriptions dataset and saves the results to .csv
    https://data.kingcounty.gov/Property-Assessments/Property-Legal-Descriptions/4854-i48r
    Outputs dataframe with the parcel_number, account_number, plat_lot_major,
    plat_block_minor, and legal_description.
    """
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata("data.kingcounty.gov", None)

    # Example authenticated client (needed for non-public datasets):
    client = Socrata('data.kingcounty.gov', APP_TOKEN, 
                     username=USERNAME, password=PASSWORD)

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("4854-i48r", limit=10000000) 

    # Convert to pandas DataFrame
    df = pd.DataFrame.from_records(results)
    
    df.to_csv(os.path.join(DATAPATH, 'property_legal_descriptions.csv'))
    
    return df

def get_gis_data():
    """
    Pulls the latest GIS data from King County
    
    ***Needs work on API, currently using .csv download from 
    https://gis-kingcounty.opendata.arcgis.com/datasets/kingcounty::parcels-for-king-county-with-address-with-property-information-parcel-address-area
    """
    # req = requests.get(GIS_GEOJSON_URL).json()
    # if req.status_code == 200:
    #     
    return

    
def parse_parcel_url(html_doc):
    """
    For a body of text pulled from King County Assessor, pull out salient
    information from html
    """
    parcel_dict = {}
    
    # Parse html to pull out tabular data
    soup = BeautifulSoup(html_doc, 'html.parser')
    table = soup.find('table', attrs={'class':'_table2'})
    try:
        for div in table.find_all('div'):
            for tr in div.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) == 2:
                    parcel_dict[tds[0].get_text(strip=True)] = \
                                                tds[1].get_text(strip=True)
        
    except Exception as e:
        print(e)
            
    return parcel_dict


def get_property_data(parcel_id):
    """
    Pulls the latest King County property data by parcel number. 
    https://data.kingcounty.gov/Property-Assessments/eReal-Property-Search/4zym-vfd2
    Must have already queried the parcel data, and will do so if it does not
    exist in data folder.
    """
    params = {'ParcelNbr': parcel_id}
    req = requests.get(EREAL_URL, params=params)
    if req.status_code == 200 and 'Site Address' in req.text:
        html_doc = req.text
        return parse_parcel_url(html_doc)
    else:
        return {}
    

def get_all_parcels():
    # Read in the parcel dataframe if it exists, otherwise pull it from King
    # County
    fname = os.path.join(DATAPATH, 'property_legal_descriptions.csv')
    if not os.path.exists(fname):
        df = get_legal_descriptions()
    else:
        df = pd.read_csv(fname, index_col=0)
        
    parcels = df['parcel_number']
    entries = []
    for i, parcel in enumerate(parcels):
        data = get_property_data(parcel)
        entries.append(data)
        if i%1000 == 0:
            print("%i PARCELS PARSED OUT OF %i" % (i, len(parcels)))
    