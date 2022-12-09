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
overdose["Year"] = overdose["Year"].astype(np.int64)

#remove explicitly missing values : shelf these
missing = overdose[(overdose["Deaths"]== "Missing")]

overdose_no_missing = overdose[~(overdose["Deaths"]== "Missing")].copy()

#deaths dtype object to int 
overdose_no_missing["Deaths"].astype("str")
overdose_no_missing["Deaths"] = overdose_no_missing.Deaths.str.replace(r"\W.","", regex=True)
overdose_no_missing["deaths"] = overdose_no_missing["Deaths"].astype("int")

#counties and states split and clean 
overdose_no_missing[["County", "State"]] = overdose_no_missing.County.str.split(",", n=1, expand=True)
overdose_no_missing["State"] = overdose_no_missing.State.str.strip()

#check Cause column
overdose_no_missing["Drug/Alcohol Induced Cause"].value_counts() #only drug poisoning found


#collapse everything to drug deaths
overdose_grouped = overdose_no_missing.groupby(["State", "County Code", "Year"])["deaths"].sum().reset_index()
overdose_grouped.head(20)
#####✅ Quality check passed##############

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

"""
implementing threshold for WA and FL
"""
#🚩 make function
#Florida Overdose
fl_states = ["FL","WV","VT","DE","HI","MT","PA", "NH","SC","NM"]
fl_states_names = ["Florida","West Virginia","Vermont","Delaware","Hawaii", "Montana","Pennsylvania","New Hampshire","South Carolina","New Mexico"]

states_FL = {
    "Florida": "FL",
    "West Virginia": "WV",
    "Vermont": "VT",
    "Delaware": "DE",
    "Hawaii": "HI",
    "Montana": "MT",
    "Pennsylvania": "PA",
    "New Hampshire": "NH",
    "South Carolina": "SC",
    "New Mexico": "NM",
}

fl_overdose = overdose_filtered[overdose_filtered["State"].isin(fl_states)].copy()
fl_overdose.rename(columns={"County Code": "fips","State":"state"}, inplace=True)
fl_overdose["fips"] = fl_overdose["fips"].astype(np.int64)
fl_overdose["year"] = fl_overdose["Year"].astype(np.int64)
fl_overdose.sort_values(by = ["state", "fips"], inplace=True)

#drop years outside 2006 and 2014 range
fl_overdose_range = fl_overdose[(fl_overdose["year"].between(2006, 2014))]

#keep only those with full 9 years
fips_in_fl = fl_overdose_range["fips"].unique()

fl_compelete_list = []
for county in fips_in_fl:
    tmp = fl_overdose_range[(fl_overdose_range["fips"] == county)]
    if len(tmp["year"]) ==9:
        fl_compelete_list.append(tmp)
        pass 
fl_overdose_complete = pd.concat(fl_compelete_list)

#check point
assert len(fl_overdose_complete["year"].unique()) == 9

assert (len(fl_overdose_complete["fips"].unique())*len(fl_overdose_complete["year"].unique())) == len(fl_overdose_complete["year"]) 

#add population to it 
#Adding 2006 to 2014 population data
#Adding 2006 to 2014 population data
pop_counties_new = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv",
    usecols=["NAME", "year", "population"]
)
pop_counties_new[["County", "State"]] = pop_counties_new.NAME.str.split(",", n=1, expand=True)
pop_counties_new["State"] = pop_counties_new.State.str.strip()

pop_new = pop_counties_new[(pop_counties_new["State"].isin(fl_states_names))]

fl_pop = pop_new[["State", "County", "year", "population"]]

#introduce fips codes to population data
fips_fl = fips[(fips["state"].isin(fl_states))].copy()
fips_fl.rename(columns={"name" : "County"}, inplace=True)

#add fips to population

pop_fl_final_list = []
for state in fl_states_names:
    tmp = fl_pop[(fl_pop["State"] == state)]
    currstate = states_FL[state]
    tmp_fips = fips_fl[(fips_fl["state"] == currstate)]
    tmp_merge = pd.merge(tmp,tmp_fips, on = "County", how = "left", indicator=True)
    pop_fl_final_list.append(tmp_merge)


pop_fl_final = pd.concat(pop_fl_final_list)
pop_fl_final_clean = pop_fl_final[["state", "fips", "County", "year", "population"]].copy()
pop_fl_final_clean.sort_values(by = ["state", "fips"], inplace=True)

#✅ both 
assert (len(pop_fl_final_clean["fips"].unique())*(len(pop_fl_final_clean["year"].unique()))) == len(pop_fl_final_clean["fips"]) 

#merge with complete drug deaths 
fl_merge_all_data = pd.merge(fl_overdose_complete,pop_fl_final_clean, on = ["fips","year"], how="left", indicator=True)
fl_merge_all_data["_merge"].value_counts()
fl_data_to_use = fl_merge_all_data[["state_x", "fips", "year", "County" , "population" , "deaths"]].copy()
fl_data_to_use.rename(columns={"state_x" : "state"}, inplace=True)
#pre data
fl_data_pre = fl_data_to_use[(fl_data_to_use["year"] <= 2010) & (fl_data_to_use["fips"] != 35013)]
fl_data_pre.to_csv('/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/Final Project/final project work/pds-2022-leep/20_intermediate_files/fl_pre.csv', index=False)


#post data 
fl_data_post = fl_data_to_use[(fl_data_to_use["year"] >= 2011) & (fl_data_to_use["fips"] != 35013)]

fl_data_post.to_csv('/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/Final Project/final project work/pds-2022-leep/20_intermediate_files/fl_post.csv', index=False)



###############################################################################################################
""" Turn into function to do WA"""

#🚩 make function
#WA Overdose
WA_states = ["WA","LA","MD","OK","IN","ID","MN","NE","NV","VA"]
WA_states_names = ["Washington", "Louisiana", "Maryland", "Oklahoma", "Indiana", "Idaho","Minnesota","Nebraska","Nevada","Virginia"]
 
states_WA = {
    "Washington": "WA",
    "Louisiana": "LA",
    "Maryland": "MD",
    "Oklahoma": "OK",
    "Indiana": "IN",
    "Idaho": "ID",
    "Minnesota": "MN",
    "Nebraska": "NE",
    "Nevada": "NV",
    "Virginia": "VA"
}

WA_overdose = overdose_filtered[overdose_filtered["State"].isin(WA_states)].copy()
WA_overdose.rename(columns={"County Code": "fips","State":"state"}, inplace=True)
WA_overdose["fips"] = WA_overdose["fips"].astype(np.int64)
WA_overdose["year"] = WA_overdose["Year"].astype(np.int64)
WA_overdose.sort_values(by = ["state", "fips"], inplace=True)

#drop years outside 2006 and 2014 range
WA_overdose_range = WA_overdose[(WA_overdose["year"].between(2006, 2014))]

#keep only those with full 9 years
fips_in_WA = WA_overdose_range["fips"].unique()

WA_compelete_list = []
for county in fips_in_WA:
    tmp = WA_overdose_range[(WA_overdose_range["fips"] == county)]
    if len(tmp["year"]) == 9:
        WA_compelete_list.append(tmp)
        pass 
WA_overdose_complete = pd.concat(WA_compelete_list)

#check point
assert len(WA_overdose_complete["year"].unique()) == 9

assert (len(WA_overdose_complete["fips"].unique())*len(WA_overdose_complete["year"].unique())) == len(WA_overdose_complete["year"]) 

#add population to it 
#Adding 2006 to 2014 population data
#Adding 2006 to 2014 population data
pop_counties_new = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv",
    usecols=["NAME", "year", "population"]
)
pop_counties_new[["County", "State"]] = pop_counties_new.NAME.str.split(",", n=1, expand=True)
pop_counties_new["State"] = pop_counties_new.State.str.strip()

pop_new = pop_counties_new[(pop_counties_new["State"].isin(WA_states_names))]

WA_pop = pop_new[["State", "County", "year", "population"]]

#introduce fips codes to population data
fips_WA = fips[(fips["state"].isin(WA_states))].copy()
fips_WA.rename(columns={"name" : "County"}, inplace=True)

#add fips to population

pop_WA_final_list = []
for state in WA_states_names:
    tmp = WA_pop[(WA_pop["State"] == state)]
    currstate = states_WA[state]
    tmp_fips = fips_WA[(fips_WA["state"] == currstate)]
    tmp_merge = pd.merge(tmp,tmp_fips, on = "County", how = "left", indicator=True)
    pop_WA_final_list.append(tmp_merge)


pop_WA_final = pd.concat(pop_WA_final_list)



pop_WA_final_clean = pop_WA_final[["state", "fips", "County", "year", "population"]].copy()
pop_WA_final_clean.sort_values(by = ["state", "fips"], inplace=True)

#✅ both  drop 51515 because no complete data on population
#assert (len(pop_WA_final_clean["fips"].unique())*(len(pop_WA_final_clean["year"].unique()))) == len(pop_WA_final_clean["fips"]) 

#merge with complete drug deaths 
WA_merge_all_data = pd.merge(WA_overdose_complete,pop_WA_final_clean, on = ["fips","year"], how="left", indicator=True)
WA_merge_all_data["_merge"].value_counts()
WA_data_to_use = WA_merge_all_data[["state_x", "fips", "year", "County" , "population" , "deaths"]].copy()
WA_data_to_use.rename(columns={"state_x" : "state"}, inplace=True)
#pre data
WA_data_pre = WA_data_to_use[(WA_data_to_use["year"] <= 2010)]
WA_data_pre.to_csv('/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/Final Project/final project work/pds-2022-leep/20_intermediate_files/WA_pre.csv', index=False)
#post data 
WA_data_post = WA_data_to_use[(WA_data_to_use["year"] >= 2011)]
WA_data_post.to_csv('/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/Final Project/final project work/pds-2022-leep/20_intermediate_files/WA_post.csv', index=False)
#🚩 some weird numbers in deaths data and missing population for  some counties, to be cross referenced
