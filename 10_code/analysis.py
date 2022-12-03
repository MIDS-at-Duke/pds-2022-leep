import altair as alt
import numpy as np
import pandas as pd

fl = pd.read_csv("/Users/emma/Downloads/Finality Subsets/FL subsetfinalized.csv")
tx = pd.read_csv("/Users/emma/Downloads/Finality Subsets/TX subsetfinalized.csv")
wa = pd.read_csv("/Users/emma/Downloads/Finality Subsets/WA subsetfinalized.csv")


def dataset(df, year, state):
    """make dataset ready to run diff-diff and pre-post for opioids"""
    df["TRANSACTION_YEAR"] = df["TRANSACTION_YEAR"].astype("int")
    groupby_1 = df.groupby(
        [
            "BUYER_STATE_x",
            "BUYER_COUNTY_x",
            "TRANSACTION_YEAR",
            "population",
            "Deaths",
        ],
        as_index=False,
    )["opioid_shipment_population_ratio"].sum()
    groupby_1["Total shipment"] = (
        groupby_1["population"] * groupby_1["opioid_shipment_population_ratio"]
    )
    opioids_data = groupby_1.groupby(
        [
            "BUYER_STATE_x",
            "TRANSACTION_YEAR",
        ],
        as_index=False,
    )["population", "Deaths", "Total shipment"].sum()
    opioids_data["death_rate"] = opioids_data["Deaths"] / opioids_data["population"]
    opioids_data["death_rate"] *= 100000
    opioids_data["opioid_shipment_population_ratio"] = (
        opioids_data["Total shipment"] / opioids_data["population"]
    )
    opioids_data["policy"] = 0
    opioids_data.loc[opioids_data["TRANSACTION_YEAR"] > year, "policy"] = 1
    opioids_data["state"] = 0
    opioids_data.loc[opioids_data["BUYER_STATE_x"] == state, "state"] = 1
    return opioids_data


FL = dataset(fl, 2010, "FL")
TX = dataset(tx, 2007, "TX")
WA = dataset(wa, 2012, "WA")

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
