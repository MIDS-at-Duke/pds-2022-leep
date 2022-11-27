"""
importing all raw data for the analysis and filtering the useful columns
"""
import pandas as pd
import datetime as dt

# Fips codes csv
fips = pd.read_csv(
    "https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv"
)
fips.sample(6)
fips.columns

# Opioids Description Dataset
# url = "https://d2ty8gaf6rmowa.cloudfront.net/dea-pain-pill-database/bulk/arcos_all_washpost.tsv.gz"
url = "/Users/lorna/Downloads/prescription_data.zip"

opioids_raw = pd.read_csv(
    url,
    chunksize=1000000,
    compression="zip",
    iterator=True,
    usecols=[
        "BUYER_STATE",
        "BUYER_COUNTY",
        "TRANSACTION_DATE",
        "CALC_BASE_WT_IN_GM",
        "MME_Conversion_Factor",
    ],
)

# states from the control function
states = ["FL", "WA", "TX", "ME", "WV", "VT", "LA", "MD", "UT", "OK", "GA", "DC"]
tmp = []
for data in opioids_raw:
    tmpdata = data[data["BUYER_STATE"].isin(states)]
    tmp.append(tmpdata)
opioids_data = pd.concat(tmp)
opioids_data.to_csv("opioids_data.csv", encoding="utf-8", index=False)


# Formatting the dates

opioids_data["TRANSACTION_DATE"].dtype  # We identified an object type
opioids_data["TRANSACTION_DATE"].sample(10)

# The values clearly do not have the same character length,
# and they will be converted to strings, in order to easily add 0s
# to the shorter values

opioids_data["TRANSACTION_DATE"] = opioids_data["TRANSACTION_DATE"].astype("str")


def date_len(element):
    """
    This function counts the characters in the TRANSACTION_DATE.
    A full date should have 8 characters, any less or more necessitates
    cleaning.
    """
    return len(element)


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
    "0" + opioids_data.loc[opioids_data.loc[:, "date_length"] == 7, "TRANSACTION_DATE"]
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


def insert_slash(element):
    """
    This function is to insert the slashes to make the dates
    more readable.
    """

    s = element

    return s[:2] + "/" + s[2:4] + "/" + s[4:]


opioids_data["TRANSACTION_DATE_DT"] = opioids_data["TRANSACTION_DATE"].apply(
    insert_slash
)

opioids_data["TRANSACTION_DATE_DT"] = pd.to_datetime(
    opioids_data["TRANSACTION_DATE_DT"]
)

# final check for the dates
opioids_data[["TRANSACTION_DATE_DT", "TRANSACTION_YEAR"]].head(10)


# The populations

popstates = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_states_20062014.csv"
)
popstates.head(25)


pop_counties = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv"
)
pop_counties.head()


# overdose data
overdose_2006 = pd.read_csv(
    "/Users/pr158admin/Desktop/Practical Data Science/project/US_VitalStatistics/Underlying Cause of Death, 2006.txt",
    sep=" ",
)

overdose_2006.head(10)

## Constant States Selection
def constant_states(states, num_constant_states, df_selection):
    """
    The function will return the Constant States we want to compare with the states of our interest.
    """

    states_elder_proportions = {}

    for s in states:
        a = df_selection.loc[
            df_selection["state"] == s,
        ]["proportion"]
        states_elder_proportions[s] = a.values.tolist()[0]
    for k, v in states_elder_proportions.items():
        df_selection[k] = df_selection["proportion"].apply(lambda x: x - v).abs()

    constant_states = {}
    for i in states:
        a = df_selection.nsmallest(num_constant_states + 1, i).state.tolist()
        del a[0]
        constant_states[i] = a

    return constant_states


# dataframe cleaning
df_selection = pd.read_csv("elder_generation_proportion_per_state.csv")
df_selection = df_selection.rename(
    columns={"0.165": "proportion", "United States": "state"}
)
df_selection = df_selection.loc[:50, :]
i = df_selection[df_selection.state == "Alaska"].index
df_selection.drop(i, inplace=True)

# Generating constant states
states = ["Florida", "Washington", "Texas"]
num_constant_states = 3
constant_states(states, 3, df_selection=df_selection)






























































#overdose deaths dataset
overdose_deaths=pd.read_csv("overdose_df.csv")
#filtering the states
states= ["FL", "WA", "TX", "ME", "WV","VT", "LA", "MD", "UT", "OK", "GA", "DC"]
#overdose_df1 = overdose_df[overdose_df["County"].isin(states)]
overdose_deaths2=overdose_deaths[overdose_deaths["County"].str.contains('|'.join(states))]
overdose_copy=overdose_deaths2.copy()
overdose_copy[['County', 'State']]= overdose_deaths2.County.str.split(",", n=1, expand = True)
overdose_copy['County']=overdose_copy['County'].str.lower()
overdose_copy["County"]=overdose_copy["County"].str.replace('county', '')
overdose_copy["County"] = overdose_copy["County"].astype(str) +' county'
overdose_copy.drop(['Notes','Year Code'], axis=1)
#final dataset to be used is overdose_copy 
