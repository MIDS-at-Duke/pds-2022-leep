import altair as alt
import numpy as np
import pandas as pd

fl = pd.read_csv("/Users/emma/Downloads/Finality Subsets/FL subsetfinalized.csv")
wa = pd.read_csv("/Users/emma/Downloads/Finality Subsets/WA subsetfinalized.csv")
FL_pre = pd.read_csv("/Users/emma/pds-2022-leep/20_intermediate_files/fl_pre.csv")
FL_post = pd.read_csv("/Users/emma/pds-2022-leep/20_intermediate_files/fl_post.csv")
FL_new = pd.concat([FL_pre, FL_post])
FL_new = FL_new.rename(columns = {"year":"TRANSACTION_YEAR", "state": "BUYER_STATE_x", "County":"BUYER_COUNTY_x", "deaths": "Deaths"})


def data_opioids(df, year, state):
    """make dataset ready to run diff-diff and pre-post for opioids"""
    df["TRANSACTION_YEAR"] = df["TRANSACTION_YEAR"].astype("int")
    df = df.groupby(
        ["BUYER_STATE_x", "BUYER_COUNTY_x", "TRANSACTION_YEAR", "population", "Deaths"],
        as_index=False,
    )["opioid_shipment_population_ratio"].sum()
    # groupby_1['Total shipment']= groupby_1['population']*groupby_1['opioid_shipment_population_ratio']
    # opioids_data = (groupby_1.groupby(['BUYER_STATE_x','TRANSACTION_YEAR',],as_index=False)['population','Deaths','Total shipment'].sum())
    df["death_rate"] = df["Deaths"] / df["population"]
    df["death_rate"] *= 100000
    df["policy"] = 0
    df.loc[df["TRANSACTION_YEAR"] > year, "policy"] = 1
    df["state"] = 0
    df.loc[df["BUYER_STATE_x"] == state, "state"] = 1
    return df


def data_death(df, year, state):
    """make dataset ready to run diff-diff and pre-post for overdose deaths"""
    df["TRANSACTION_YEAR"] = df["TRANSACTION_YEAR"].astype("int")
    df["Deaths"] = df["Deaths"].astype("int")
    df["death_rate"] = df["Deaths"] / df["population"]
    df["death_rate"] *= 100000
    df = df.groupby(
        ["BUYER_STATE_x", "BUYER_COUNTY_x", "TRANSACTION_YEAR"], as_index=False
    )["death_rate"].sum()
    df["policy"] = 0
    df.loc[df["TRANSACTION_YEAR"] > year, "policy"] = 1
    df["state"] = 0
    df.loc[df["BUYER_STATE_x"] == state, "state"] = 1
    return df


FL = data_opioids(fl, 2010, "FL")
FL_death = data_death(FL_new, 2010, "FL")
WA = data_opioids(wa, 2012, "WA")

# Pre-Post
def pre_post(data, yvar, xvar, year, analysis, target, alpha=0.05):
    import statsmodels.formula.api as smf

    # Grid for predicted values
    data1 = data.loc[(data["policy"] == 0) & (data["state"] == 1), :]
    x = data1.loc[pd.notnull(data1[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions1 = pd.DataFrame({xvar: grid})

    # Fit model_before, get predictions
    model_before = smf.ols(f"{yvar} ~ {xvar}", data=data1).fit()
    model_predict = model_before.get_prediction(predictions1[xvar])
    predictions1[yvar] = model_predict.summary_frame()["mean"]
    predictions1[["ci_low", "ci_high"]] = model_predict.conf_int(alpha=alpha)

    # Build chart
    reg1 = (
        alt.Chart(predictions1)
        .mark_line()
        .encode(x=xvar, y=alt.Y(yvar, scale=alt.Scale(zero=False)))
    )
    ci1 = (
        alt.Chart(predictions1)
        .mark_errorband()
        .encode(
            x=xvar,
            y=alt.Y("ci_low", axis=alt.Axis(title=target)),
            y2="ci_high",
        )
    )

    data2 = data.loc[(data["policy"] == 1) & (data["state"] == 1), :]
    x = data2.loc[pd.notnull(data2[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions2 = pd.DataFrame({xvar: grid})

    # Fit model_before, get predictions
    model_after = smf.ols(f"{yvar} ~ {xvar}", data=data2).fit()
    model_predict_after = model_after.get_prediction(predictions2[xvar])
    predictions2[yvar] = model_predict_after.summary_frame()["mean"]
    predictions2[["ci_low", "ci_high"]] = model_predict_after.conf_int(alpha=alpha)

    # Build chart
    reg2 = (
        alt.Chart(predictions2)
        .mark_line()
        .encode(x=alt.X(xvar, title="Year"), y=alt.Y(yvar, scale=alt.Scale(zero=False)))
    )
    ci2 = (
        alt.Chart(predictions2)
        .mark_errorband()
        .encode(
            x=xvar,
            y=alt.Y("ci_low", axis=alt.Axis(title=target)),
            y2="ci_high",
        )
    )

    overlay = pd.DataFrame({"x": [year]})
    vline = alt.Chart(overlay).mark_rule(color="red", strokeWidth=3).encode(x="x:Q")

    chart = alt.layer(ci1, ci2, reg1, reg2, vline).properties(title=analysis)

    return predictions1, chart


# Diff-Diff func


def diff_diff(data, yvar, xvar, year, analysis, target, alpha=0.05):
    import statsmodels.formula.api as smf

    # Grid for predicted values
    data1 = data.loc[(data["policy"] == 0) & (data["state"] == 0), :]
    x = data1.loc[pd.notnull(data1[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions1 = pd.DataFrame({xvar: grid})

    # Fit model_before, get predictions
    model_before = smf.ols(f"{yvar} ~ {xvar}", data=data1).fit()
    model_predict = model_before.get_prediction(predictions1[xvar])
    predictions1[yvar] = model_predict.summary_frame()["mean"]
    predictions1[["ci_low", "ci_high"]] = model_predict.conf_int(alpha=alpha)

    # Build chart
    reg1 = (
        alt.Chart(predictions1)
        .mark_line(color="green")
        .encode(x=xvar, y=alt.Y(yvar, scale=alt.Scale(zero=False)))
    )
    ci1 = (
        alt.Chart(predictions1)
        .mark_errorband(color="green")
        .encode(
            x=xvar,
            y=alt.Y("ci_low", axis=alt.Axis(title=target)),
            y2="ci_high",
        )
    )

    data11 = data.loc[(data["policy"] == 0) & (data["state"] == 1), :]
    x = data11.loc[pd.notnull(data11[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions11 = pd.DataFrame({xvar: grid})

    # Fit model_before, get predictions
    model_before1 = smf.ols(f"{yvar} ~ {xvar}", data=data11).fit()
    model_predict1 = model_before1.get_prediction(predictions11[xvar])
    predictions11[yvar] = model_predict1.summary_frame()["mean"]
    predictions11[["ci_low", "ci_high"]] = model_predict1.conf_int(alpha=alpha)

    # Build chart
    reg11 = (
        alt.Chart(predictions11)
        .mark_line()
        .encode(x=xvar, y=alt.Y(yvar, scale=alt.Scale(zero=False)))
    )
    ci11 = (
        alt.Chart(predictions11)
        .mark_errorband()
        .encode(
            x=xvar,
            y=alt.Y("ci_low", axis=alt.Axis(title=target)),
            y2="ci_high",
        )
    )

    data2 = data.loc[(data["policy"] == 1) & (data["state"] == 0), :]
    x = data2.loc[pd.notnull(data2[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions2 = pd.DataFrame({xvar: grid})

    # Fit model_before, get predictions
    model_after = smf.ols(f"{yvar} ~ {xvar}", data=data2).fit()
    model_predict_after = model_after.get_prediction(predictions2[xvar])
    predictions2[yvar] = model_predict_after.summary_frame()["mean"]
    predictions2[["ci_low", "ci_high"]] = model_predict_after.conf_int(alpha=alpha)

    # Build chart
    reg2 = (
        alt.Chart(predictions2)
        .mark_line(color="green")
        .encode(x=xvar, y=alt.Y(yvar, scale=alt.Scale(zero=False)))
    )
    ci2 = (
        alt.Chart(predictions2)
        .mark_errorband(color="green")
        .encode(x=xvar, y=alt.Y("ci_low", axis=alt.Axis(title=target)), y2="ci_high")
    )

    data21 = data.loc[(data["policy"] == 1) & (data["state"] == 1), :]
    x = data21.loc[pd.notnull(data21[yvar]), xvar]
    xmin = x.min()
    xmax = x.max()
    step = (xmax - xmin) / 100
    grid = np.arange(xmin, xmax + step, step)
    predictions21 = pd.DataFrame({xvar: grid})

    # Fit model_before, get predictions
    model_after1 = smf.ols(f"{yvar} ~ {xvar}", data=data21).fit()
    model_predict_after1 = model_after1.get_prediction(predictions21[xvar])
    predictions21[yvar] = model_predict_after1.summary_frame()["mean"]
    predictions21[["ci_low", "ci_high"]] = model_predict_after1.conf_int(alpha=alpha)

    # Build chart
    reg21 = (
        alt.Chart(predictions21).mark_line().encode(x=alt.X(xvar, title="Year"), y=yvar)
    )
    ci21 = (
        alt.Chart(predictions21)
        .mark_errorband()
        .encode(
            x=xvar,
            y=alt.Y("ci_low", axis=alt.Axis(title=target)),
            y2="ci_high",
        )
    )

    overlay = pd.DataFrame({"x": [year]})
    vline = alt.Chart(overlay).mark_rule(color="red", strokeWidth=2).encode(x="x:Q")

    chart = alt.layer(ci1, ci2, ci11, ci21, reg1, reg11, reg2, reg21, vline).properties(
        title=analysis
    )

    return chart


# Florida
xvar = "TRANSACTION_YEAR"
yvar = "opioid_shipment_population_ratio"
analysis = "Pre-Post Analysis For Florida"
target = "Opioids per capita"
fit, chart = pre_post(FL, yvar, xvar, 2010, analysis, target, alpha=0.05)
chart

xvar = "TRANSACTION_YEAR"
yvar = "opioid_shipment_population_ratio"
analysis = "Difference-Difference Analysis For Florida"
target = "Opioids per capita"
chart = diff_diff(FL, yvar, xvar, 2010, analysis, target)
chart

xvar = "TRANSACTION_YEAR"
yvar = "death_rate"
analysis = "Pre-Post Analysis For Florida"
target = "Overdose Death Rate"
fit, chart = pre_post(FL, yvar, xvar, 2010, analysis, target, alpha=0.05)
chart

# Washington

xvar = "TRANSACTION_YEAR"
yvar = "death_rate"
analysis = "Difference-Diference Analysis For Florida"
target = "Overdose Death Rate"
chart = diff_diff(FL, yvar, xvar, 2010, analysis, target, alpha=0.05)
chart

xvar = "TRANSACTION_YEAR"
yvar = "opioid_shipment_population_ratio"
analysis = "Pre-Post Analysis For Washington"
target = "Opioids per capita"
fit, chart = pre_post(WA, yvar, xvar, 2012, analysis, target, alpha=0.05)
chart

xvar = "TRANSACTION_YEAR"
yvar = "opioid_shipment_population_ratio"
analysis = "Difference-Difference Analysis For Washington"
target = "Opioids per capita"
chart = diff_diff(WA, yvar, xvar, 2012, analysis, target)
chart

xvar = "TRANSACTION_YEAR"
yvar = "death_rate"
analysis = "Pre-Post Analysis For Washington"
target = "Overdose Death Rate"
fit, chart = pre_post(WA, yvar, xvar, 2012, analysis, target, alpha=0.05)
chart

xvar = "TRANSACTION_YEAR"
yvar = "death_rate"
analysis = "Difference-Difference Analysis For Washington"
target = "Overdose Death Rate"
chart = diff_diff(WA, yvar, xvar, 2012, analysis, target)
chart

# Texas
# merging two pop datasets
pop_counties = pd.read_csv(
    "https://raw.githubusercontent.com/wpinvestigative/arcos-api/master/data/pop_counties_20062014.csv",
    usecols=["countyfips", "year", "population"],
)
pop_2003 = pd.read_csv(
    "/Users/emma/pds-2022-leep/00_source_data/2000_2006_pop.csv",
    usecols=[
        "state",
        "county",
        "popestimate2003",
        "popestimate2004",
        "popestimate2005",
    ],
)
death = pd.read_csv("/Users/emma/pds-2022-leep/00_source_data/overdose_df.csv")
fips = pd.read_csv(
    "https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv"
)
fips = fips.rename({"fips": "countyfips", "name": "county"}, axis=1)
pop_counties.year = pop_counties.year.astype("int")
pop_counties.countyfips = pop_counties.countyfips.astype("int")
pop_2003["county"] = pop_2003["county"].astype("str")
pop_2003["county"] = pop_2003["county"].str.strip().str.zfill(3)
pop_2003["state"] = pop_2003["state"].astype("str")
pop_2003["state"] = pop_2003["state"].str.strip().str.zfill(2)
pop_2003["fips"] = pop_2003["state"] + pop_2003["county"]
pop_2003 = pop_2003.drop(pop_2003.columns[[0, 1]], axis=1)
pop_2003 = pop_2003.rename(
    {"popestimate2003": "2003", "popestimate2004": "2004", "popestimate2005": "2005"},
    axis=1,
)
new = pd.melt(pop_2003, id_vars=["fips"], value_vars=["2003", "2004", "2005"])
pop_3 = new.rename(
    {"fips": "countyfips", "variable": "year", "value": "population"}, axis=1
)
pop_3.year = pop_3.year.astype("int")
pop_3.countyfips = pop_3.countyfips.astype("int")
pop_counties = pd.concat([pop_counties, pop_3], axis=0)
death["County Code"] = death["County Code"].astype("int")
death["Year"] = death["Year"].astype("int")
death = death.drop(death.columns[[0, 1, 4, 5, 6]], axis=1)
death = death.rename({"County Code": "countyfips", "Year": "year"}, axis=1)
df1 = death.merge(pop_counties, how="left")
df_death_capita = df1.merge(fips, how="left")

# selecting TX constant states
state = ["TX", "UT", "GA", "CO", "CA", "ND", "IL", "LA", "MD", "OK"]
death_df = pd.DataFrame()
for state in state:
    state_df = df_death_capita.loc[df_death_capita["state"] == state, :]
    death_df = pd.concat([death_df, state_df], axis=0)
death_df["Deaths"] = death_df.Deaths.astype("float")
death_df["death_rate"] = death_df["Deaths"] / death_df["population"]
death_df["death_rate"] *= 100000
death_df = death_df.rename({"state": "state_abbr"}, axis=1)
death_df["policy"] = 0
death_df.loc[death_df["year"] > 2007, "policy"] = 1
death_df["state"] = 0
death_df.loc[death_df["state_abbr"] == "TX", "state"] = 1

# Pre-post
xvar = "year"
yvar = "death_rate"
analysis = "Pre-Post Analysis For Texas"
target = "Overdose Death Rate"
fit, chart = pre_post(death_df, yvar, xvar, 2007, analysis, target, alpha=0.05)
chart

# Diff-Diff
xvar = "year"
yvar = "death_rate"
analysis = "Difference-Difference Analysis For Texas"
target = "Overdose Death Rate"
chart = diff_diff(death_df, yvar, xvar, 2007, analysis, target, alpha=0.05)
chart


# Estimation


def estimate_diff(df, target):
    """doing calculations for the effect of policy change (based on diff-diff analysis)"""
    effect = []
    for state in [0, 1]:
        df_pre = df.loc[(df["policy"] == 0) & (df["state"] == state), :]
        pre = df_pre[target].mean()
        print(f"the prepolicy for {target} is {pre}")
        df_post = df.loc[(df["policy"] == 1) & (df["state"] == state), :]
        post = df_post[target].mean()
        print(f"the postpolicy for {target} is {post}")
        effect.append(pre - post)
        print(effect)
    return effect[1] - effect[0]


print(estimate_diff(WA, "opioid_shipment_population_ratio"))
print(estimate_diff(WA, "death_rate"))
print(estimate_diff(FL, "opioid_shipment_population_ratio"))
print(estimate_diff(FL, "death_rate"))
print(estimate_diff(death_df, "death_rate"))  # For Texas


# Summary Statistics

# Florida
summarystat_fl = FL.drop(
    ["Deaths", "Total shipment", "population", "TRANSACTION_YEAR"], axis=1
)
result_FL = summarystat_fl.groupby(
    [
        "state",
        "policy",
    ],
    as_index=False,
).agg(["mean", "median", "min", "max"])

# Washington

summarystat_wa = WA.drop(
    ["Deaths", "Total shipment", "population", "TRANSACTION_YEAR"], axis=1
)
result_WA = summarystat_wa.groupby(
    [
        "state",
        "policy",
    ],
    as_index=False,
).agg(["mean", "median", "min", "max"])

# Texas
summarystat_tx = death_df.drop(death_df.columns[[0, 1, 2, 3, 4, 5]], axis=1)
result_TX = summarystat_tx.groupby(
    [
        "state",
        "policy",
    ],
    as_index=False,
).agg(["mean", "median", "min", "max"])
