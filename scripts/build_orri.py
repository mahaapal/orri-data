import os
import pandas as pd
from fredapi import Fred

fred = Fred(api_key=os.environ["FRED_API_KEY"])

SERIES = {
    "TEMPHELPS": "TEMPHELPS",            # Temp help employment (monthly, SA)
    "HTRUCKSSA": "HTRUCKSSA",            # Heavy weight truck sales (monthly, SA)
    "CES7072200001": "CES7072200001",    # Restaurants & bars jobs (monthly, SA)
    "AWHAEMAN": "AWHAEMAN",              # Mfg weekly hours (monthly, SA)
    "BUSINV": "BUSINV",                  # Business inventories (monthly)
    "BUSTOTSLS": "BUSTOTSLS",            # Business sales (monthly)
    "ICSA": "ICSA",                      # Initial claims (weekly, SA)
    "RAILFRTCARLOADSD11": "RAILFRTCARLOADSD11"  # Rail carloads (monthly, SA)
}

def get_monthly(series_id, start="1995-01-01"):
    """Fetch a FRED series and return month-end frequency.
       Weekly/daily series are end-of-month via .last()."""
    s = fred.get_series(series_id, observation_start=start)
    s = s.to_frame(series_id)
    s.index = pd.to_datetime(s.index)
    return s.resample("ME").last()   # month-end

def sign_trend(series, months=6):
    """+1 if rising vs N months ago, -1 if falling, 0 if flat/NaN."""
    if len(series) <= months:
        return 0
    cur = series.iloc[-1]
    prev = series.iloc[-months]
    if pd.isna(cur) or pd.isna(prev):
        return 0
    return 1 if cur > prev else (-1 if cur < prev else 0)

def main():
    frames = []
    for name, code in SERIES.items():
        try:
            frames.append(get_monthly(code).rename(columns={code: name}))
        except Exception as e:
            raise RuntimeError(f"Failed fetching {name} ({code}): {e}")

    df = pd.concat(frames, axis=1)

    # Inventory-to-Sales ratio
    df["INV_SALES_RATIO"] = df["BUSINV"] / df["BUSTOTSLS"]

    # ORRI score (sum of component trend signs; invert bad = rising claims & rising I/S)
    scores = []
    for i in range(len(df)):
        sub = df.iloc[: i+1]
        if len(sub) < 7:
            scores.append(float("nan"))
            continue
        score = 0
        score += sign_trend(sub["TEMPHELPS"])
        score += sign_trend(sub["HTRUCKSSA"])
        score += sign_trend(sub["CES7072200001"])
        score += -sign_trend(sub["ICSA"])                # rising claims = bad
        score += sign_trend(sub["RAILFRTCARLOADSD11"])
        score += -sign_trend(sub["INV_SALES_RATIO"])     # rising I/S = bad
        score += sign_trend(sub["AWHAEMAN"])
        scores.append(score)

    df["ORRI"] = scores

    os.makedirs("data", exist_ok=True)
    df.to_csv("data/orri.csv", index_label="Date")

if __name__ == "__main__":
    main()

