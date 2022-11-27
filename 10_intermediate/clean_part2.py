import pandas as pd
import datetime as dt
import numpy as np

# The cleaning continues here after

# opioid_data_fips_pop_deaths_nonan = pd.read_csv("mergescompletedwithzeronancounties.csv")

opioid_data_fips_pop_deaths_nonan = pd.read_csv(
    "mergescompletedwithzeronancounties.csv",
    chunksize=10000000,
    iterator=True,
    usecols=[
        "BUYER_STATE_x",
        "BUYER_COUNTY_x",
        "TRANSACTION_YEAR",
        "TRANSACTION_DATE_DT",
        "Opioids_Shipment_IN_GM",
        "fips",
        "population",
        "Deaths",
    ],
)

tmp = []
for data in opioid_data_fips_pop_deaths_nonan:
    # Replacing missing deaths with 0s, as 0 overdose deaths for those years
    data.fillna(value=0, axis=1, inplace=True)
    tmp.append(data)

opioids_fips_pop_death_cleaned = pd.concat(tmp)

opioids_fips_pop_death_cleaned.sample(10)

opioids_fips_pop_death_cleaned.dtypes

opioids_fips_pop_death_cleaned.isna().sum()

# Mission Success after Vejigantes knows how many crashes

# Opioids Shipment divided by population

opioids_fips_pop_death_cleaned[
    "opioid_shipment_population_ratio"
] = opioids_fips_pop_death_cleaned["Opioids_Shipment_IN_GM"].astype(
    "float"
) / opioids_fips_pop_death_cleaned[
    "population"
].astype(
    "float"
)

# Florida': ['Maine', 'West Virginia', 'Vermont'], ME, WV, VT
#'Washington': ['Louisiana', 'Maryland', 'Oklahoma'], LA, MD, OK
#'Texas': ['Colorado', 'Utah', 'Georgia'] DC, UT, CO

# add datetime conversion code, not priority right now

states_dn = {
    "FL": ["FL", "ME", "WV", "VT"],
    "WA": ["WA", "LA", "MD", "OK"],
    "TX": ["TX", "GA", "UT", "CO"],
}
for state in states_dn:

    target = opioids_fips_pop_death_cleaned.loc[
        opioids_fips_pop_death_cleaned["BUYER_STATE_x"].isin(states_dn[state]), :
    ]

    target.to_csv(f"{state +' subsetmaximized'}.csv", encoding="utf-8", index=False)


############## group by code for the states #########################


##################################################################


# group by state, county and sum of opioids shipment make it opioids clean : pending to add a year so that its by year
opioid_data_fips_pop_deaths = (
    opioid_data_fips.groupby(["BUYER_STATE", "BUYER_COUNTY"])["Opioids_Shipment_IN_GM"]
    .sum()
    .reset_index()
)

# 🚩 DC has a problem: Investigating DC data


# Diagnostics Code

# opioid_data_fips['fips'].head(10) - Revealed it was a float column

# opioid_data_fips['fips'].value_counts(dropna=False) -  Revealed 5228720 NANS values

# fips[fips['BUYER_STATE']=='FL'] - Took this as a case study, we wanted "saint johns county" from it

# fips.loc[fips['BUYER_STATE']=='FL','BUYER_COUNTY'].unique() - "saint johns" was "st. johns" in the fips

# opioid_data_fips.loc[opioid_data_fips['_merge']=='left_only', 'BUYER_COUNTY'].unique()


# LA : ascension county ->
# opioid_data_fips.loc[(opioid_data_fips['BUYER_COUNTY'] == 'nan county') | (opioid_data_fips['BUYER_COUNTY'].isnull())]

# def asserting_left_only(df):

#     assert len(df.loc[df['_merge']=='left_only', 'BUYER_COUNTY'].unique()) == 2

#     for null_val in df.loc[df['_merge']=='left_only', 'BUYER_COUNTY'].unique():

#         assert null_val in ['nan county', np.nan]

#     pass
