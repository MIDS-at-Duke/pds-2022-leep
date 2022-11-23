'''
importing all raw data for the analysis and filtering the useful columns
'''
import pandas as pd

#Fips codes csv
fips = pd.read_csv("https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv")
fips.sample(6)
fips.columns

#Opioids Description Dataset
# url = "https://d2ty8gaf6rmowa.cloudfront.net/dea-pain-pill-database/bulk/arcos_all_washpost.tsv.gz"
url = r"C:\Users\ericr\Downloads\cmder\IDS720LEEP\pds-2022-leep\arcos_all_washpost.tsv"

opioids_raw = pd.read_csv(url, chunksize= 1000000, sep="\t", iterator=True, usecols = ["BUYER_STATE","BUYER_COUNTY", "TRANSACTION_DATE", "CALC_BASE_WT_IN_GM","MME_Conversion_Factor"])
states = ["FL","WA","TX"]
tmp = []
for data in opioids_raw:
    tmpdata = data[data["BUYER_STATE"].isin(states)]
    tmp.append(tmpdata)
opioids_data = pd.concat(tmp)
opioids_data.to_csv("opioids_data.csv", encoding='utf-8', index=False)

#The populations 

popstates = pd.read_csv('https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_states_20062014.csv')
popstates.head(25)


pop_counties = pd.read_csv('https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv')
pop_counties.head()


























#overdose data
overdose_2006 = pd.read_csv("/Users/pr158admin/Desktop/Practical Data Science/project/US_VitalStatistics/Underlying Cause of Death, 2006.txt", sep =" ")

overdose_2006.head(10)
