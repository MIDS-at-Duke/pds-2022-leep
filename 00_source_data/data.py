'''
importing all raw data for the analysis and filtering the useful columns
'''
import pandas as pd

#Fips codes csv
fips = pd.read_csv("https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv")
fips.sample(6)
fips.columns

#Opioids Description Dataset
opioids_raw = pd.read_csv("https://d2ty8gaf6rmowa.cloudfront.net/dea-pain-pill-database/bulk/arcos_all_washpost.tsv.gz", compression="gzip", chunksize=5000000,usecols = ["BUYER_STATE","BUYER_COUNTY", "TRANSCATION_DATE", "CALC_BASE_WT_IN_GM","MME_Conversion_Factor"]) 

states = ["FL","WA","TX"]


