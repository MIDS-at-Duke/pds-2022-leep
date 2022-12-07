"""
Implementing the analysis threshold.
Output: counties to drop from analysis and the threshold of analysis
Lorna, Fall 2022
"""

import pandas as pd 
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
overdose_grouped.head(50)

#filter out states of interest, OK and LA appear twice, WV and VA corrected
states = ["FL","WV","VT","DE","HI","MT","PA","NH","SC","NM","WA","IN","ID","MN","NE","NV","VA","TX","UT","GA","CO","CA","ND","IL","LA","MD","OK"]

overdose_filtered = overdose_grouped[overdose_grouped["State"].isin(states)]

comparison = overdose_filtered.State.unique()

assert set(states) == set(comparison)


"""
less than 10 deaths and missing deaths will be dropped
counties will also be dropped from the shipment for the same regard. 
"""

#🚩 north dakota
#how many counties per state?
overdose[(overdose["State"] == "ND")]

