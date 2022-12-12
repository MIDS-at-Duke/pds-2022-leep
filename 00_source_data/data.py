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

# The populations

popstates = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_states_20062014.csv"
)
popstates.head(25)

# We needed more years for population counties

pop_counties_new = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv"
)
pop_counties_new.head(25)


# pop_counties_old = pd.read_excel(
#     "https://repository.duke.edu/download/f49b199b-1496-4636-91f3-36226c8e7f80",
#     usecols=["fips", "year", "tot_pop"],
# )

# for Texas:
pop_counties_old = pd.read_csv(
    "https://www2.census.gov/programs-surveys/popest/datasets/2000-2006/counties/totals/co-est2006-alldata.csv"
)


# dictionary of states and abbreviations

states_mapper = pd.read_csv(
    "https://worldpopulationreview.com/static/states/name-abbr.csv", header=None
)

state, abb = states_mapper[0].to_list(), states_mapper[1].to_list()

states_map = {}

for i in range(0, len(state)):

    states_map[(state[i]).lower()] = abb[i]


# overdose data
overdose_2006 = pd.read_csv(
    "/Users/pr158admin/Desktop/Practical Data Science/project/US_VitalStatistics/Underlying Cause of Death, 2006.txt",
    sep=" ",
)

overdose_2006.head(10)

## Constant States Selection, the comparison states generator


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

# Record of initial trials, the first control states

# Florida': ['Maine', 'West Virginia', 'Vermont'], ME, WV, VT
#'Washington': ['Louisiana', 'Maryland', 'Oklahoma'], LA, MD, OK
#'Texas': ['Colorado', 'Utah', 'Georgia'] GA, UT, CO

# Chnaged states to 10
# 'Florida': ['Maine','West Virginia','Vermont','Delaware','Hawaii','Montana','Pennsylvania','New Hampshire','South Carolina','New Mexico'], ME, WV, VT
# 'Washington': ['Louisiana', 'Maryland', 'Oklahoma', 'Washington','Indiana','Idaho', 'Minnesota','Nebraska','Nevada','Virginia'],LA, MD, OK
# Texas': ['Utah', 'Georgia', 'Colorado', 'California', 'North Dakota', 'Illinois','Louisiana','Maryland', 'Oklahoma','Washington']}GA, UT, CO

################################################################################################
# Important :

# Virginia are separate states. West Virginia and Virginia being present in the dataset is not
# a mistake.
# References: https://www.whereig.com/usa/states/virginia/counties/
# http://www.virginiaplaces.org/regions/westva.html
# https://en.wikipedia.org/wiki/History_of_West_Virginia

################################################################################################

# parameter set to 12, constructing the lists and dictionaries to house the control and treatment states

treatment_control_pairings = constant_states(states, 10, df_selection=df_selection)

treatment_control_lists = []

final_states = set()

for treatment_state in treatment_control_pairings:

    states_ls = [i for i in treatment_control_pairings[treatment_state]]

    # sanity check

    treatment_states_ls = [i for i in treatment_control_pairings.keys()]

    treatment_states_ls.remove(treatment_state)

    for ts in treatment_states_ls:  # aLways 2 elements

        if ts in states_ls:

            states_ls.remove(ts)

            # No treatment states passing as control states

    print(f"{treatment_state} now has {len(states_ls)} control states.")

    states_ls = [treatment_state] + states_ls

    pairings_with_abbreviations = {}

    for state in states_ls:

        if state.lower() in states_map:

            final_states.add(states_map[state.lower()])

            pairings_with_abbreviations[state] = states_map[state.lower()]

    treatment_control_lists.append([pairings_with_abbreviations])

print(f"Number of final states : {len(final_states)}.")

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

assert len(opioids_data["BUYER_STATE"].unique()) == len(final_states)

for fin_state in opioids_data["BUYER_STATE"].unique():

    assert fin_state in final_states

opioids_data.to_csv(
    r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\00_source_data\opioids_data.csv.zip",
    compression="zip",
    encoding="utf-8",
    index=False,
)

# test = pd.read_csv("00_source_data/opioids_data.csv", compression='gzip', encoding="utf-8")

# Closing Final Chunking Process


# Preserving this cleaning code for posterity, but it was handled and optimized in the
# threshold.py file by the other members.

# # overdose deaths dataset
# overdose_deaths = pd.read_csv(
#     r"C:\Users\ericr\Downloads\cmder\720newsafeopioids\pds-2022-leep\00_source_data\overdose_df.csv"
# )

# # overdose_df1 = overdose_df[overdose_df["County"].isin(states)]
# overdose_deaths2 = overdose_deaths[
#     overdose_deaths["County"].str.contains("|".join(final_states))
# ]
# overdose_copy = overdose_deaths2.copy()
# overdose_copy[["County", "State"]] = overdose_deaths2.County.str.split(
#     ",", n=1, expand=True
# )
# overdose_copy["County"] = overdose_copy["County"].str.lower()
# overdose_copy["County"] = overdose_copy["County"].str.replace("county", "")
# overdose_copy["County"] = overdose_copy["County"].astype(str) + " county"
# overdose_copy = overdose_copy.drop(["Notes", "Year Code"], axis=1)
# overdose_copy["Deaths"] = overdose_copy["Deaths"].astype("float")

# # 8001.0 county code, year 2009 :  52 + 15 deaths = 67 , this is our reference
# # this was done to prevent severe data bloating

# overdose_grouped = (
#     overdose_copy.groupby(["County Code", "Year"])["Deaths"].sum().reset_index()
# )

# overdose_grouped.to_csv("overdosegrouped.csv", encoding="utf-8", index=False)
# # final overdose deaths dataset to be used is overdose_copy
