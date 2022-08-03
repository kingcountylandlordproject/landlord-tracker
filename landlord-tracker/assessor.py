import numpy as np
import pandas as pd
import os

fname = r"C:\Users\sydne\Documents\DSA\king-county-housing\landlord-tracker\data\EXTR_RPAcct_220706.csv"
df = pd.read_csv(fname)
df = df.set_index('AcctNbr')

legal_fname = r"C:\Users\sydne\Documents\DSA\king-county-housing\landlord-tracker\data\property_legal_descriptions.csv"
legal = pd.read_csv(legal_fname)
legal = legal[['account_number', 'parcel_number', 'legal_description']].set_index('account_number')
df = df.join(legal, how='outer')

res_fname = r"C:\Users\sydne\Documents\DSA\king-county-housing\landlord-tracker\data\Residential Building\EXTR_ResBldg.csv"
res = pd.read_csv(res_fname)