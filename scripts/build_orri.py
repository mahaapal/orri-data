import os, math
import pandas as pd
from fredapi import Fred

fred = Fred(api_key=os.environ["FRED_API_KEY"])

SERIES = [
    "TEMPHELPS",        # temp help employment
    "TRUCKD11",         # heavy truck sales
    "CES7072200001",    # restaurants & bars employment
    "AWHAEMAN",         # mfg weekly hours
    "BUSINV",           # business inventories
    "BUSTOTSLS",        # business sales
    "ICSA",             # initial claims (weekly)
    "RAILFRTCARLOAD"    # weekly rail carloads
]

def monthly(series_id, start="1995-01-01"):
    # monthly frequency; EOP aggregation for weekly series
    s = fred.get_series(series_id, observation_start=start)
    s = s.to_frame(series_id)
    s.index = pd.to_datetime(s.index)
    m = s.resample("M").last()  # end-of-period
    return m

def sign_trend(series, months=6):
    # +1 if rising vs N months ago, -1 if falling, 0 if flat/na
    cur = series.iloc[-1]
    prev = series.iloc[-months] if len(series) > months else None
    if pd.isna(cur) or pd.isna(prev):
        return 0
    if cur > prev: return 1
    if cur < prev: return -1
    return 0

def main():
    dfs = [monthly(s) for s in SERIES]
    df = pd.concat(dfs, axis=1)
    df["INV_SALES_RATIO"] = df["BUSINV"] / df["BUSTOTSLS"]
    df = df.dropna(how="all")
    df.to_csv("data/orri_components.csv", index_label="Date")

    # simple ORRI scoring (customize as you like)
    scores = []
    for i in range(len(df)):
        sub = df.iloc[: i+1]
        if len(sub) < 7: 
            scores.append(float("nan")); continue
        score = 0
        score += sign_trend(sub["TEMPHELPS"])
        score += sign_trend(sub["TRUCKD11"])
        score += sign_trend(sub["CES7072200001"])
        score += -sign_trend(sub["ICSA"])               # claims rising is BAD
        score += sign_trend(sub["RAILFRTCARLOAD"])
        score += -sign_trend(sub["INV_SALES_RATIO"])    # rising I/S is BAD
        score += sign_trend(sub["AWHAEMAN"])
        scores.append(score)
    out = df[["TEMPHELPS","TRUCKD11","CES7072200001","AWHAEMAN","ICSA","RAILFRTCARLOAD","INV_SALES_RATIO"]].copy()
    out["ORRI"] = scores
    out.to_csv("data/orri.csv", index_label="Date")

if __name__ == "__main__":
    main()
