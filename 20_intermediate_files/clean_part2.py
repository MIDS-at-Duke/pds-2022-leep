import pandas as pd
import datetime as dt
import numpy as np
import gc

# The cleaning continues here after

# opioid_data_fips_pop_deaths_nonan = pd.read_csv("mergescompletedwithzeronancounties.csv")

opioid_data_fips_pop_deaths_nonan = pd.read_csv(
    "mergescompletedwithzeronancounties.csv",
    chunksize=1000000,
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

del tmp
gc.collect()

opioids_fips_pop_death_cleaned.sample(10)

opioids_fips_pop_death_cleaned.dtypes

opioids_fips_pop_death_cleaned.isna().sum()

# Opioids Shipment divided by population, recalculated for typing

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
    "Virginia": "VA",
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

    target = opioids_fips_pop_death_cleaned.loc[
        opioids_fips_pop_death_cleaned["BUYER_STATE_x"].isin(new_ls), :
    ]

    target.to_csv(f"{new_ls[0] +' subsetfinalized'}.csv", encoding="utf-8", index=False)




