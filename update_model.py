import pandas as pd
from miscs.utils import merge_data
from miscs.update_historical import fetch_historical_data

app = fetch_historical_data(symbols=["EURUSD"], frames={"15 mins": "m15"})
last_week = app.df["eurusd_m15"]

# ------------------------- LOAD HISTORICAL DATA ------------------------- #

name = 'eurusd_m15_historical'
folder = 'datasets/'

historical = pd.read_csv(f"{folder}{name}.csv", compression='gzip')
historical = historical[['date', 'open', 'high', 'low', 'close']].round(5)

historical['date'] = pd.to_datetime(historical['date'], format='mixed')
historical = historical.set_index('date')

# --------------------------- MERGE LAST WEEK --------------------------- #

historical = merge_data(historical, last_week)

historical_ = historical.reset_index()
historical_.to_csv(f"{folder}{name}.csv", index=False, compression='gzip')

print("Historical data upadated up to!:", historical_.index[-1])