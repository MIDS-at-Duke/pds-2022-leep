import pandas as pd
import gc

FL = pd.read_csv(r"C:\Users\Eric\Downloads\Finality Subsets\FL subsetfinalized.csv")

WA = pd.read_csv(r"C:\Users\Eric\Downloads\Finality Subsets\WA subsetfinalized.csv")


FL_OvFiltered = pd.read_csv(
    r"C:\Users\Eric\Downloads\leep\pds-2022-leep\20_intermediate_files\fl_full.csv"
)

WA_OvFiltered = pd.read_csv(
    r"C:\Users\Eric\Downloads\leep\pds-2022-leep\20_intermediate_files\WA_full.csv"
)


FL.drop(columns=["population", "Deaths"], inplace=True)
WA.drop(columns=["population", "Deaths"], inplace=True)
FL_OvFiltered_cols = FL_OvFiltered[["fips", "year", "population", "deaths"]]
WA_OvFiltered_cols = WA_OvFiltered[["fips", "year", "population", "deaths"]]


FL_merge = pd.merge(
    FL,
    FL_OvFiltered_cols,
    how="right",
    left_on=["fips", "TRANSACTION_YEAR"],
    right_on=["fips", "year"],
    indicator=True,
)


WA_merge = pd.merge(
    WA,
    WA_OvFiltered_cols,
    how="right",
    left_on=["fips", "TRANSACTION_YEAR"],
    right_on=["fips", "year"],
    indicator=True,
)

FL_merge["_merge"].value_counts()
WA_merge["_merge"].value_counts()

assert len(FL_merge[FL_merge["_merge"] == "both"]) == 25957129
assert len(WA_merge[WA_merge["_merge"] == "both"]) == 15906766

FL_DEC10_subset = FL_merge[FL_merge["_merge"] == "both"]
WA_DEC10_subset = WA_merge[WA_merge["_merge"] == "both"]

FL_DEC10_subset = FL_DEC10_subset.drop(columns=["_merge"], inplace=False)
WA_DEC10_subset = WA_DEC10_subset.drop(columns=["_merge"], inplace=False)

sum(FL_DEC10_subset.duplicated())
sum(WA_DEC10_subset.duplicated())

FL_DEC10_subset.drop_duplicates(inplace=True)
WA_DEC10_subset.drop_duplicates(inplace=True)

assert sum(FL_DEC10_subset.duplicated()) == 0
assert sum(WA_DEC10_subset.duplicated()) == 0

# Sanity checks to see what fips are missing in the final subsets
set(FL_DEC10_subset["fips"]).difference(set(FL_OvFiltered_cols["fips"]))
set(FL_OvFiltered_cols["fips"]).difference(set(FL_DEC10_subset["fips"]))
missing_fips_FLOvF = list(
    set(FL_OvFiltered_cols["fips"]).difference(set(FL_DEC10_subset["fips"]))
)
FL_OvFiltered[FL_OvFiltered["fips"].isin(missing_fips_FLOvF)]

set(WA_DEC10_subset["fips"]).difference(set(WA_OvFiltered_cols["fips"]))
set(WA_OvFiltered_cols["fips"]).difference(set(WA_DEC10_subset["fips"]))
missing_fips_WAOvF = list(
    set(WA_OvFiltered_cols["fips"]).difference(set(WA_DEC10_subset["fips"]))
)
WA_OvFiltered[WA_OvFiltered["fips"].isin(missing_fips_WAOvF)]

FL_DEC10_subset.to_csv("FL_DEC10_subset.csv", encoding="utf-8", index=False)
WA_DEC10_subset.to_csv("WA_DEC10_subset.csv", encoding="utf-8", index=False)
