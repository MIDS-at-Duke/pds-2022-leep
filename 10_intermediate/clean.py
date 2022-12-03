import pandas as pd
import datetime as dt
import numpy as np
import gc

url = r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\00_source_data\opioids_data.csv"

# Final Buffer States

final_states = [
    "WV",
    "SC",
    "DE ",
    "HI",
    "NM",
    "GA",
    "IN",
    "MN",
    "VT",
    "OK",
    "LA",
    "UT",
    "MD",
    "FL",
    "NV",
    "PA",
    "IL",
    "CO",
    "MT",
    "TX",
    "WA",
    "CA",
    "NH",
    "ID",
    "ND",
    "NE",
]


first_view = pd.read_csv(url, usecols=["BUYER_COUNTY", "BUYER_STATE"])

# Check if they states lists are identical
assert final_states.sort() == (first_view["BUYER_STATE"].unique()).sort()

del first_view
gc.collect()

opioids_data_chunker = pd.read_csv(url, chunksize=1000000, iterator=True)

# Formatting the dates


def date_len(element):
    """
    This function counts the characters in the TRANSACTION_DATE.
    A full date should have 8 characters, any less or more necessitates
    cleaning.
    """
    return len(element)


def insert_slash(element):
    """
    This function is to insert the slashes to make the dates
    more readable.
    """

    s = element

    return s[:2] + "/" + s[2:4] + "/" + s[4:]


to_concat = []

for opioids_data in opioids_data_chunker:

    opioids_data["TRANSACTION_DATE"].dtype  # We identified an object type
    opioids_data["TRANSACTION_DATE"].sample(10)

    # The values clearly do not have the same character length,
    # and they will be converted to strings, in order to easily add 0s
    # to the shorter values

    opioids_data["TRANSACTION_DATE"] = opioids_data["TRANSACTION_DATE"].astype("str")

    # def date_len(element):
    #     """
    #     This function counts the characters in the TRANSACTION_DATE.
    #     A full date should have 8 characters, any less or more necessitates
    #     cleaning.
    #     """
    #     return len(element)

    # Creating a new column to identify the shorter strings
    opioids_data["date_length"] = opioids_data["TRANSACTION_DATE"].apply(date_len)
    opioids_data[["TRANSACTION_DATE", "date_length"]].sample(10)

    # Date strings with 8,9,10, and 7 characters.
    opioids_data["date_length"].value_counts()

    # Turning them all to floats and then back to integers to easily remove the decimal points,
    # then back to strings.
    # Now verify the new value_counts of the lengths after recalculating the date_length column
    opioids_data["TRANSACTION_DATE"] = opioids_data["TRANSACTION_DATE"].astype("float")
    opioids_data["TRANSACTION_DATE"] = opioids_data["TRANSACTION_DATE"].astype("int")
    opioids_data["TRANSACTION_DATE"] = opioids_data["TRANSACTION_DATE"].astype("str")

    # Change successful, only 8 and 7 character strings.
    # Now we just have to fix the 7 date strings.
    opioids_data["date_length"] = opioids_data["TRANSACTION_DATE"].apply(date_len)
    opioids_data["date_length"].value_counts()

    # adding the 0s to the shorter strings, so they have 8 characters
    opioids_data.loc[opioids_data.loc[:, "date_length"] == 7, "TRANSACTION_DATE"] = (
        "0"
        + opioids_data.loc[opioids_data.loc[:, "date_length"] == 7, "TRANSACTION_DATE"]
    )

    # testing the changes
    opioids_data["date_length"] = opioids_data["TRANSACTION_DATE"].apply(date_len)
    assert sum(opioids_data["date_length"] == 8) == opioids_data.shape[0]

    # assuming the assert passed, all of our date strings now have 8 characters.
    # dropping the placeholder date_length column
    opioids_data = opioids_data.drop(columns="date_length")

    # extracting the year for easy referencing and grouping
    opioids_data["TRANSACTION_YEAR"] = opioids_data["TRANSACTION_DATE"].str.extract(
        "[0-9]{4}(20[0-9]{2})"
    )

    opioids_data["TRANSACTION_YEAR"] = opioids_data["TRANSACTION_YEAR"].astype(int)

    # datetime version of TRANSACTION_DATE column creation
    # This is crucial for filtering by months

    # def insert_slash(element):
    #     """
    #     This function is to insert the slashes to make the dates
    #     more readable.
    #     """

    #     s = element

    #     return s[:2] + "/" + s[2:4] + "/" + s[4:]

    opioids_data["TRANSACTION_DATE_DT"] = opioids_data["TRANSACTION_DATE"].apply(
        insert_slash
    )

    opioids_data["TRANSACTION_DATE_DT"] = pd.to_datetime(
        opioids_data["TRANSACTION_DATE_DT"]
    )

    # final check for the dates
    opioids_data[["TRANSACTION_DATE_DT", "TRANSACTION_YEAR"]].head(10)

    # create a MME per WT to standardize opioids per transaction

    opioids_data["CALC_BASE_WT_IN_GM"].isnull().values.any()

    opioids_data["MME_Conversion_Factor"].isnull().values.any()

    opioids_data["Opioids_Shipment_IN_GM"] = opioids_data["CALC_BASE_WT_IN_GM"].astype(
        "float"
    ) * opioids_data["MME_Conversion_Factor"].astype("float")

    # For future FIPS code merging into Opioids dataset
    opioids_data["BUYER_COUNTY"] = opioids_data["BUYER_COUNTY"].astype(str) + " county"
    opioids_data["BUYER_COUNTY"] = opioids_data["BUYER_COUNTY"].str.lower()

    to_concat.append(opioids_data)

opioids_data = pd.concat(to_concat)

del opioids_data_chunker, to_concat
gc.collect()


# Modifying bad county names in general

fips_mapper = {
    "saint johns county": "st. johns county",
    "saint lucie county": "st. lucie county",
    "de soto county": "desoto county",
    "prince georges county": "prince george's county",
    "queen annes county": "queen anne's county",
    "saint marys county": "st. mary's county",
    "de witt county": "dewitt county",
}

for bad_county in fips_mapper:

    opioids_data.loc[
        opioids_data["BUYER_COUNTY"] == bad_county, "BUYER_COUNTY"
    ] = fips_mapper[bad_county]

# Modifying for bad Lousiana County names
# they didn't match because they did not have parish in their names
# a previous solution of removing them became an issue only in this particular instance, something to note

opioids_data.loc[opioids_data["BUYER_STATE"] == "LA", "BUYER_COUNTY"] = (
    opioids_data.loc[opioids_data["BUYER_STATE"] == "LA", "BUYER_COUNTY"]
    .str.replace("parish", "")
    .str.extract("(.*) county")
    + " parish county"
)

# Fips codes csv
fips = pd.read_csv(
    "https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv"
)
# additing FIPS codes
fips.name = fips.name.str.lower()
fips = fips.rename(columns={"name": "BUYER_COUNTY", "state": "BUYER_STATE"})
fips = fips.replace(to_replace=" county", value="", regex=True)
fips["BUYER_COUNTY"] = fips["BUYER_COUNTY"].astype(str) + " county"
opioid_data_fips = pd.merge(
    opioids_data, fips, how="left", on=["BUYER_STATE", "BUYER_COUNTY"], indicator=True
)

opioid_data_fips.drop(columns="_merge", inplace=True)

# opioid_data_fips.drop(columns="_merge", inplace=True)

del opioids_data
gc.collect()

# Our Backup
opioid_data_fips.to_csv("merge1.csv", encoding="utf-8", index=False)

# Merging populations, oddity : year becomes a float

# pop_counties has too many columns, only going to keep essential ones

pop_counties_new = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv"
)

opioid_data_fips_pop = pd.merge(
    opioid_data_fips,
    pop_counties_new,
    how="left",
    left_on=["fips", "TRANSACTION_YEAR"],
    right_on=["countyfips", "year"],
    indicator=True,
)

opioid_data_fips_pop.drop(columns="_merge", inplace=True)

# Not yet fully tested to work with members' implementations
# opioid_data_fips_pop = pd.merge(
#     opioid_data_fips,
#     pop_counties_old,
#     how="left",
#     left_on=["fips", "TRANSACTION_YEAR"],
#     right_on=["fips", "year"],
#     indicator=True,
# )

del opioid_data_fips
gc.collect()

opioid_data_fips_pop.to_csv("opioid_fips_pop.csv", encoding="utf-8", index=False)





# Merging the deaths with FIPS first, then with opioid_data_fips_pop

overdose_grouped = pd.read_csv(r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\10_intermediate\overdosegrouped.csv")

deaths_fips = pd.merge(
    overdose_grouped,
    fips,
    how="left",
    left_on=["County Code"],
    right_on=["fips"],
    indicator=True,
)

deaths_fips.drop(columns="_merge", inplace=True)

deaths_fips.to_csv("deathfipsmerge.csv", encoding="utf-8", index=False)

# If need to write csvs at this point :
# opioid_data_fips_pop =  pd.read_csv(r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\00_source_data\opioid_fips_pop.csv")
# deaths_fips = pd.read_csv(r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\00_source_data\deathfipsmerge.csv")


opioid_data_fips_pop_deaths = pd.merge(
    opioid_data_fips_pop,
    deaths_fips,
    how="left",
    left_on=["fips", "TRANSACTION_YEAR"],
    right_on=["fips", "Year"],
    indicator=True,
)

del opioid_data_fips_pop, deaths_fips
gc.collect()

# Early Writing

states_FL = {
    "Florida": "FL",
    "West Virginia": "WV",
    "Vermont": "VT",
    "Delaware": "DE ",
    "Hawaii": "HI",
    "Montana": "MT",
    "Pennsylvania": "PA",
    "New Hampshire": "NH",
    "South Carolina": "SC",
    "New Mexico": "NM",
}
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
    "Virginia": "WV",
}
states_TX = {
    "Texas": "TX",
    "Utah": "UT",
    "Georgia": "GA",
    "Colorado": "CO",
    "California": "CA",
    "North Dakota": "ND",
    "Illinois": "IL",
    "Louisiana": "LA",
    "Maryland": "MD",
    "Oklahoma": "OK",
}

states_pairings = {"TX": [], "WA": [], "FL": []}

for states_dn in [states_FL, states_WA, states_TX]:

    new_ls = []

    for state in states_dn:

        new_ls.append(states_dn[state])

    target = opioid_data_fips_pop_deaths.loc[
        opioid_data_fips_pop_deaths["BUYER_STATE_x"].isin(new_ls), :
    ]

    target.to_csv(f"{new_ls[0] +' subsetprepv1'}.csv", encoding="utf-8", index=False)


opioid_data_fips_pop_deaths.to_csv(
    f"mergescompletednocleaning.csv", encoding="utf-8", index=False
)


# These were verified manually, some of these counties had no drug related deaths
# sometimes there is missing data. Leaving in the _merge column for now
missing_deaths = opioid_data_fips_pop_deaths.loc[
    opioid_data_fips_pop_deaths["_merge"] == "left_only", :
]
missing_deaths["fips"].unique()

del missing_deaths
gc.collect()

# Dropping the nan counties
opioid_data_fips_pop_deaths_nonan = opioid_data_fips_pop_deaths[
    ~(opioid_data_fips_pop_deaths["BUYER_COUNTY_x"].isin(["nan county", np.nan]))
]


# assert (
#     opioid_data_fips_pop_deaths.shape[0] - opioid_data_fips_pop_deaths_nonan.shape[0]
#     == 4359979
# )

opioid_data_fips_pop_deaths_nonan.to_csv(
    f"mergescompletedwithzeronancounties.csv", encoding="utf-8", index=False
)

###############################################################
# Computer Cutsoff HERE!!!!!!!!!!!!!!!!!!!!

# Replacing missing deaths with 0s, as 0 overdose deaths for those years

opioid_data_fips_pop_deaths_nonan.fillna(value=0, axis=1, inplace=True)

# Florida': ['Maine', 'West Virginia', 'Vermont'], ME, WV, VT
#'Washington': ['Louisiana', 'Maryland', 'Oklahoma'], LA, MD, OK
#'Texas': ['Colorado', 'Utah', 'Georgia'] DC, UT, CO

states_dn = {
    "FL": ["FL", "ME", "WV", "VT"],
    "WA": ["WA", "LA", "MD", "OK"],
    "TX": ["TX", "GA", "UT", "CO"],
}
for state in states_dn:

    target = opioid_data_fips_pop_deaths_nonan.loc[
        opioid_data_fips_pop_deaths_nonan["BUYER_STATE_x"].isin(states_dn[state]), :
    ]

    target.to_csv(f"{state +' subsetcleaned'}.csv", encoding="utf-8", index=False)


###########################################################################################

# The grouping happens on  analysis.py,
# the other notes below are for future reference and improvements

###########################################################################################


### Demo of implementing the code to fix the lacking deaths years for Texas:

# Creating placeholder years to help Texas have actual years pre and post

# unique_counties_for_state = {}

# concat_me = [['BUYER_STATE', 'BUYER_COUNTY', 'TRANSACTION_DATE', 'CALC_BASE_WT_IN_GM',
#        'MME_Conversion_Factor']]

# for state in opioids_data['BUYER_STATE'].unique().tolist():

#     unique_counties_for_state[state] = opioids_data.loc[opioids_data['BUYER_STATE']==state,'BUYER_COUNTY'].unique().tolist()

# for state_key in unique_counties_for_state:

#     for county in unique_counties_for_state[state_key]:

#         for year in range(2003,2006):

#             # concat_me.append([state_key, county, f'1015{year}', 0, 0])

#             # opioids_data = opioids_data.append({'BUYER_STATE' : state_key, 'BUYER_COUNTY' : county, 'TRANSACTION_DATE' : f'1015{year}', 'CALC_BASE_WT_IN_GM' : 0,
#             # 'MME_Conversion_Factor': 0}, ignore_index=True)

#             concat_me.append([state_key, county, f'1015{year}',  0, 0]) # adds the empty years that Emma needed

# df2 = pd.DataFrame(concat_me[1:], columns=concat_me[0])

# opioids_data = pd.concat([opioids_data, df2], sort=False)


# Diagnostics Code : Troubleshooting past issues

# opioid_data_fips['fips'].head(10) - Revealed it was a float column

# opioid_data_fips['fips'].value_counts(dropna=False) -  Revealed 5228720 NANS values

# fips[fips['BUYER_STATE']=='FL'] - Took this as a case study, we wanted "saint johns county" from it

# fips.loc[fips['BUYER_STATE']=='FL','BUYER_COUNTY'].unique() - "saint johns" was "st. johns" in the fips

# opioid_data_fips.loc[opioid_data_fips['_merge']=='left_only', 'BUYER_COUNTY'].unique()


# Writing half of the assert function we were initially planning on using, if not
# for time constraints

# def asserting_left_only(df):

#     assert len(df.loc[df['_merge']=='left_only', 'BUYER_COUNTY'].unique()) == 2

#     for null_val in df.loc[df['_merge']=='left_only', 'BUYER_COUNTY'].unique():

#         assert null_val in ['nan county', np.nan]

#     pass
