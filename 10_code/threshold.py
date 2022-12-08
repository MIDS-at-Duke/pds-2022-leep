"""
Implementing the analysis threshold.
Output: counties to drop from analysis and the threshold of analysis
Lorna, Fall 2022
"""

import pandas as pd 
import numpy as np
from collections import defaultdict


#Import overdose deaths 

overdose = pd.read_csv("/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/Final Project/final project work/pds-2022-leep/00_source_data/overdose_df.csv")
overdose.head(15)

#clean the data set > State, County, Code, Year, Deaths

#s.str.split(r".", expand=True)

#Examining column by column

overdose.columns

#no duplicates in county code
assert overdose["County Code"].duplicated().any()

overdose["County Code"].astype("int")

#Change year to date time 
overdose["Year"].unique() #2009 to 2013
overdose["Year"].astype("int")

#counties and states split and clean 
overdose[["County", "State"]] = overdose.County.str.split(",", n=1, expand=True)
overdose["State"] = overdose.State.str.strip()

#check Cause column
overdose["Drug/Alcohol Induced Cause"].value_counts() #only drug poisoning found

#collapse everything to drug deaths
overdose_grouped = overdose.groupby(["State", "County Code", "Year"])["Deaths"].sum().reset_index()
overdose_grouped.head(20)


#filter out states of interest, OK and LA appear twice, WV and VA corrected
states = ["FL","WV","VT","DE","HI","MT","PA","NH","SC","NM","WA","IN","ID","MN","NE","NV","VA","TX","UT","GA","CO","CA","ND","IL","LA","MD","OK"]

overdose_filtered = overdose_grouped[overdose_grouped["State"].isin(states)]
overdose_filtered["County Code"].astype("int")

assert set(states) == set(overdose_filtered.State.unique())


"""
less than 10 deaths and missing deaths will be dropped
counties will also be dropped from the shipment for the same regard. 
"""

#🚩 north dakota
#how many counties per state?
overdose[(overdose["State"] == "ND")]


#using fips codes
fips = pd.read_csv(
    "https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv"
)
fips.head(8)

 
 #find missing counties and add them to a separate df

missing_counties = []

for state in states:
    tmp = overdose_filtered[(overdose_filtered["State"] == state)]
    tmp_fips = fips[(fips["state"] == state)]
    county_present = tmp["County Code"].unique()
    county_comparison = tmp_fips["fips"]
    missing = []
    for i in county_comparison:
        if i not in county_present:
            missing.append(i)
        pass
    State = [state]*len(missing)
    tmp_df = pd.DataFrame()
    tmp_df["State"] = State
    tmp_df["County Fips"] = missing
    missing_counties.append(tmp_df)

missing_data = pd.concat(missing_counties)


#take them out of our filtered df 
overdose_filtered_no_missing = overdose_filtered[~(overdose_filtered["County Code"].isin([missing_data["County Fips"]]))]

overdose_filtered_no_missing["Deaths"].min()
overdose_filtered_no_missing["Deaths"].isna().any()


"""
Texas, deaths from 2003 to 2014, threshold is 2007
Control states = "TX","UT","GA","CO", "CA","ND","IL", "LA","MD","OK",
"""
group_tx = ["TX","UT","GA","CO", "CA","ND","IL", "LA","MD","OK"]
group_tx_names = ["Texas","Utah","Georgia","Colorado", "California","North Dakota","Illinois","Louisiana","Maryland","Oklahoma"]

deaths_tx = overdose_filtered_no_missing[(overdose_filtered_no_missing["State"].isin(group_tx))]

# for Texas:
#2003 to 2005 population data
pop_counties_old = pd.read_csv(
    "https://www2.census.gov/programs-surveys/popest/datasets/2000-2006/counties/totals/co-est2006-alldata.csv", usecols=["stname","ctyname", "popestimate2003","popestimate2004","popestimate2005"])

pop_old = pop_counties_old[(pop_counties_old["stname"].isin(group_tx_names)) & (~ (pop_counties_old["ctyname"].isin(group_tx_names)))]

pop_tx_old = pd.melt(pop_old, id_vars=["stname","ctyname",], var_name="year", value_name="population")

pop_tx_old["year"] = pop_tx_old.year.str.replace("popestimate","")
pop_tx_old["year"] = pop_tx_old.year.str.strip()
pop_tx_old["year"].value_counts() #884 counties ✅
pop_tx_old.rename(columns={"stname": "State", "ctyname":"County"}, inplace=True)

#pop_tx_old.sort_values(by = ["stname","ctyname"])

#Adding 2006 to 2014 population data
pop_counties_new = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv",
    usecols=["NAME", "year", "population"]
)
pop_counties_new[["County", "State"]] = pop_counties_new.NAME.str.split(",", n=1, expand=True)
pop_counties_new["State"] = pop_counties_new.State.str.strip()

pop_new = pop_counties_new[(pop_counties_new["State"].isin(group_tx_names))]

pop_tx_new = pop_new[["State", "County", "year", "population"]]

pop_tx_new["year"].value_counts() #884 counties ✅

assert sorted(pop_tx_new["State"].unique()) == sorted(group_tx_names)

#merge to make a texas population dataset
pop_tx_merged = pd.concat([pop_tx_old, pop_tx_new])
pop_tx_merged.sort_values(by = ["State", "County"], inplace=True)


#introduce fips codes to population data
fips_tx = fips[(fips["state"].isin(group_tx))].copy()
fips_tx.rename(columns={"name" : "County"}, inplace=True)

#add fips to population
pop_tx_fips = pd.merge(fips_tx, pop_tx_merged, on = "County", how = "left", indicator=True)
pop_tx_fips["_merge"].value_counts() #✅ both 
pop_tx_final = pop_tx_fips[["state", "fips", "County", "year", "population"]].copy()

#reintroduce overdose data

overdose_tx = deaths_tx[(~(deaths_tx["Year"] == 2015))].copy()
overdose_tx.rename(columns={"County Code": "fips","State":"state"}, inplace=True)
overdose_tx["fips"] = overdose_tx["fips"].astype(np.int64)
overdose_tx["year"] = overdose_tx["Year"].astype(np.int64)
overdose_tx.sort_values(by = ["state", "fips"], inplace=True)

#overdose 2003 to 2005
overdose_tx_03 = overdose_tx[(overdose_tx["year"] <= 2005)]

#overdose 2006 to 2011
overdose_tx_06 = overdose_tx[(overdose_tx["year"]>= 2006)]



#check for counties with full overdose data
fips_tx_group = overdose_tx_06["fips"].unique()


counties_full_overdose_data = []
for county in fips_tx_group:
    tmp = overdose_tx_06[(overdose_tx_06["fips"] == county)].copy()
    if len(tmp["year"]) == 9:
        tmp1 = pop_tx_fips[(pop_tx_fips["fips"] == county)]
        #assert len(tmp1["year"]) == 12
        tmp_merge = pd.merge(tmp, tmp1, on = "year", how = "left")
        counties_full_overdose_data.append(tmp_merge)
        pass


testfin = pd.concat(counties_full_overdose_data)
###########################################################################################

full_pop_data = pop_tx_final[(pop_tx_final["fips"].isin(counties_full_overdose_data))].reset_index()

full_overdose_data = overdose_tx[(overdose_tx["fips"].isin(counties_full_overdose_data))].reset_index()

final_tx_merge = pd.merge(full_overdose_data,pop_tx_final,on = ["fips"], how = "left", indicator=True)

final_tx_merge["_merge"].value_counts()

#🚩 texas analysis still failing to concat 2003 and 2006



