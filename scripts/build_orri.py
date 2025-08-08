# scripts/build_orri.py
import os
import pandas as pd
from fredapi import Fred

# --- Config ---
START_DATE = "1995-01-01"

# FRED series (all valid as of 2025-08)
SERIES = {
    "TEMPHELPS": "TEMPHELPS",                 # Temp help employment (SA, monthly)
    "HTRUCKSSA": "HTRUCKSSA",                 # Heavy truck sales (SA, monthly, level)
    "CES7072200001": "CES7072200001",         # Restaurants & bars employment (SA, monthly)
    "AWHAEMAN": "AWHAEMAN",                   # Manufacturing weekly hours (SA, monthly)
    "TOTBUSSMSA": "TOTBUSSMSA",               # Total business sales (SA, monthly)
    "ICSA": "ICSA",                           # Initial jobless claims (SA, weekly)
    "RAILFRTCARLOADSD11": "RAILFRTCARLOADSD11",  # Rail carloads (SA, monthly)
    "ISRATIO": "ISRATIO",                     # Inventories-to-Sales ratio (Total business, monthly)
}

# ORRI logic: +1 if rising vs 6 months ago, -1 if falling, 0 otherwise
# "Bad when rising": claims (ICSA) and ISRATIO (inventories piling up).
BAD_WHEN_RISING = {"ICSA", "ISRATIO"}
LOOKBACK_MONTHS = 6

def get_env_api_key():
    key = os.environ.get("FRED_API_KEY")
    if not key:
        raise RuntimeError("Missing FRED_API_KEY environment variable")
    return key

def fetch_series_month_end(fred: Fred, series_id: str, start=START_DATE) -> pd.DataFrame:
    """Fetch series and return a month-end (ME) indexed DataFrame with the series_id as column."""
    s = fred.get_series(series_id, observation_start=start)
    if s is None or len(s) == 0:
        raise RuntimeError(f"No data returned for {series_id}")
    df = s.to_frame(series_id)
    df.index = pd.to_datetime(df.index)
    # Ensure month-end frequency (ME replaces deprecated "M")
    df = df.resample("ME").last()
    return df

def sign_trend(series: pd.Series, months: int = LOOKBACK_MONTHS) -> int:
    """Return +1, 0, -1 comparing latest to N months ago."""
    if series.isna().any() or len(series) <= months:
        return 0
    cur = series.iloc[-1]
    prev = series.iloc[-months]
    if pd.isna(cur) or pd.isna(prev):
        return 0
    if cur > prev: return 1
    if cur < prev: return -1
    return 0

def main():
    fred = Fred(api_key=get_env_api_key())

    # Fetch all series
    frames = []
    for name, code in SERIES.items():
        df = fetch_series_month_end(fred, code)
        frames.append(df.rename(columns={code: name}))

    # Combine on month-end index
    data = pd.concat(frames, axis=1)

    # Compute ORRI as sum of component trend signs (invert "bad-when-rising")
    orri_vals = []
    for i in range(len(data)):
        sub = data.iloc[: i + 1]
        if len(sub) <= LOOKBACK_MONTHS:
            orri_vals.append(float("nan"))
            continue
        score = 0
        for col in SERIES.keys():
            sig = sign_trend(sub[col])
            if col in BAD_WHEN_RISING:
                sig = -sig
            score += sig
        orri_vals.append(score)

    out = data.copy()
    out["ORRI"] = orri_vals

    # Save
    os.makedirs("data", exist_ok=True)
    out.to_csv("data/orri.csv", index_label="Date")
    print("Wrote data/orri.csv")

if __name__ == "__main__":
    main()


