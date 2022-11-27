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

# For the following code to work on your local machines due to the ram overhead issue,
# import the csv you got from the above code block, I'll leave it commented for now

# opioids_data = pd.read_csv("opioids_data.csv")


# The populations

popstates = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_states_20062014.csv"
)
popstates.head(25)


pop_counties = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv"
)
pop_counties.head(25)


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
i = df_selection[df_selection.state == "District of Columbia"].index
df_selection.drop(i, inplace=True)

# Generating constant states
states = ["Florida", "Washington", "Texas"]
num_constant_states = 3
constant_states(states, 3, df_selection=df_selection)
# Florida': ['Maine', 'West Virginia', 'Vermont'], ME, WV, VT
#'Washington': ['Louisiana', 'Maryland', 'Oklahoma'], LA, MD, OK
#'Texas': ['Colorado', 'Utah', 'Georgia'] DC, UT, CO


# overdose deaths dataset
overdose_deaths = pd.read_csv("overdose_df.csv")
# filtering the states
states = ["FL", "WA", "TX", "ME", "WV", "VT", "LA", "MD", "UT", "OK", "GA", "CO"]
# overdose_df1 = overdose_df[overdose_df["County"].isin(states)]
overdose_deaths2 = overdose_deaths[
    overdose_deaths["County"].str.contains("|".join(states))
]
overdose_copy = overdose_deaths2.copy()
overdose_copy[["County", "State"]] = overdose_deaths2.County.str.split(
    ",", n=1, expand=True
)
overdose_copy["County"] = overdose_copy["County"].str.lower()
overdose_copy["County"] = overdose_copy["County"].str.replace("county", "")
overdose_copy["County"] = overdose_copy["County"].astype(str) + " county"
overdose_copy.drop(["Notes", "Year Code"], axis=1)
# final dataset to be used is overdose_copy
