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
url = r"C:\Users\Eric\Downloads\prescription_data.zip"

opioids_raw = pd.read_csv(
    url,
    chunksize=10000000,
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
states = ["FL", "WA", "TX", "ME", "WV", "VT", "LA", "MD", "UT", "OK", "GA", "CO"]
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

states_mapper = pd.read_csv(
    "https://worldpopulationreview.com/static/states/name-abbr.csv", header=None
)

state, abb = states_mapper[0].to_list(), states_mapper[1].to_list()

states_map = {}

for i in range(0, len(state)):

    states_map[state[i]] = abb[i]

states_map

pop_counties_new = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv"
)
pop_counties_new.head(25)


pop_counties_old = pd.read_excel(
    "https://repository.duke.edu/download/f49b199b-1496-4636-91f3-36226c8e7f80",
    usecols=["fips", "year", "tot_pop"],
)


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
num_constant_states = 10
constant_states(states, 10, df_selection=df_selection)
# Florida': ['Maine', 'West Virginia', 'Vermont'], ME, WV, VT
#'Washington': ['Louisiana', 'Maryland', 'Oklahoma'], LA, MD, OK
#'Texas': ['Colorado', 'Utah', 'Georgia'] GA, UT, CO

# Chnaged states to 10
# 'Florida': ['Maine','West Virginia','Vermont','Delaware','Hawaii','Montana','Pennsylvania','New Hampshire','South Carolina','New Mexico'], ME, WV, VT
# 'Washington': ['Louisiana', 'Maryland', 'Oklahoma', 'Washington','Indiana','Idaho', 'Minnesota','Nebraska','Nevada','Virginia'],LA, MD, OK
# Texas': ['Utah', 'Georgia', 'Colorado', 'California', 'North Dakota', 'Illinois','Louisiana','Maryland', 'Oklahoma','Washington']}GA, UT, CO

states_dic = {
    "Florida": "FL",
    "West Virginia": "WV",
    "Vermont": " VT",
    "Delaware": "DE ",
    "Hawaii": "HI",
    "Montana": "MT",
    "Pennsylvania": "PA",
    "New Hampshire": "NH",
    "South Carolina": "SC",
    "New Mexico": "NM",
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

states_dn_full = {}

for states_groups in [states_FL, states_WA, states_TX]:

    states_dn_full.update(states_groups)

states_dn_full

states_set = set()

for key in states_dn_full:

    states_set.add(states_dn_full[key])

states_set

final_states = list(states_set)


# Final Chunking process

final_url = r"C:\Users\ericr\Downloads\prescription_data.zip"

opioids_raw = pd.read_csv(
    final_url,
    chunksize=10000000,
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
tmp = []
for data in opioids_raw:
    tmpdata = data[data["BUYER_STATE"].isin(final_states)]
    tmp.append(tmpdata)
opioids_data = pd.concat(tmp)
opioids_data.to_csv("opioids_data.csv", encoding="utf-8", index=False)


# Closing Final Chunking Process


# overdose deaths dataset
overdose_deaths = pd.read_csv(
    r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\00_source_data\overdose_df.csv"
)
# filtering the states
states = ["FL", "WA", "TX", "ME", "WV", "VT", "LA", "MD", "UT", "OK", "GA", "CO"]

# overdose_df1 = overdose_df[overdose_df["County"].isin(states)]
overdose_deaths2 = overdose_deaths[
    overdose_deaths["County"].str.contains("|".join(final_states))
]
overdose_copy = overdose_deaths2.copy()
overdose_copy[["County", "State"]] = overdose_deaths2.County.str.split(
    ",", n=1, expand=True
)
overdose_copy["County"] = overdose_copy["County"].str.lower()
overdose_copy["County"] = overdose_copy["County"].str.replace("county", "")
overdose_copy["County"] = overdose_copy["County"].astype(str) + " county"
overdose_copy = overdose_copy.drop(["Notes", "Year Code"], axis=1)
overdose_copy["Deaths"] = overdose_copy["Deaths"].astype("float")

# 8001.0 county code, year 2009 :  52 + 15 deaths = 67 , this is our reference
# this was done to prevent severe data bloating

overdose_grouped = (
    overdose_copy.groupby(["County Code", "Year"])["Deaths"].sum().reset_index()
)

# final dataset to be used is overdose_copy
