import pandas as pd
url = "/Users/lorna/Downloads/prescription_data.zip"

states = ["FL", "WA", "TX", "ME", "WV","VT", "LA", "MD", "UT", "OK", "GA", "DC"]

opioids_data = pd.read_csv("/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/Final Project/pds-2022-leep/00_source_data/opioids_data.csv")

# Check if they states lists are identical 
assert states.sort() == (opioids_data["BUYER_STATE"].unique()).sort()


# Formatting the dates

opioids_data["TRANSACTION_DATE"].dtype  # We identified an int64 type
opioids_data["TRANSACTION_DATE"].head(2)

# The values do not have the same length, and they will be converted to strings to add 0s to the shorter values
opioids_data["TRANSACTION_DATE"] = opioids_data["TRANSACTION_DATE"].astype("str")


def date_len(element):

    return len(element)


# Creating a new column to identify the shorter strings
opioids_data["date_length"] = opioids_data["TRANSACTION_DATE"].apply(date_len)
opioids_data[["TRANSACTION_DATE", "date_length"]].head()

# adding the 0s to the shorter strings
opioids_data.loc[opioids_data.loc[:, "date_length"] == 7, "TRANSACTION_DATE"] = (
    "0" + opioids_data.loc[opioids_data.loc[:, "date_length"] == 7, "TRANSACTION_DATE"]
)

# testing the changes
opioids_data["date_length"] = opioids_data["TRANSACTION_DATE"].apply(date_len)
assert sum(opioids_data["date_length"] == 8) == opioids_data.shape[0]

# dropping the placeholder column
opioids_data = opioids_data.drop(columns="date_length")

# extracting the year for easy referencing and grouping
opioids_data["TRANSACTION_YEAR"] = opioids_data["TRANSACTION_DATE"].str.extract(
    "[0-9]{4}(20[0-9]{2})"
)

opioids_data["TRANSACTION_YEAR"] = opioids_data["TRANSACTION_YEAR"].astype(int)

# datetime version of TRANSACTION_DATE column creation


def insert_slash(element):

    s = element

    return s[:2] + "/" + s[2:4] + "/" + s[4:]


opioids_data["TRANSACTION_DATE_DT"] = opioids_data["TRANSACTION_DATE"].apply(
    insert_slash
)

opioids_data["TRANSACTION_DATE_DT"] = pd.to_datetime(
    opioids_data["TRANSACTION_DATE_DT"]
)


#create a MME per WT to standardize opioids per transaction

opioids_data["CALC_BASE_WT_IN_GM"].isnull().values.any()

opioids_data["MME_Conversion_Factor"].isnull().values.any()

opioids_data["Opioids_Shipment_IN_GM"] = opioids_data["CALC_BASE_WT_IN_GM"]*opioids_data["MME_Conversion_Factor"]

opioids_data.sample(10)


