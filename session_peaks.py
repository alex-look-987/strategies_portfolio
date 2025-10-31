import numpy as np
import pandas as pd
from miscs.utils import data_mgmt
from features.features import session

df = data_mgmt('datasets/eurusd_m15_historical', False, 2025, 2, 2025, 2)

df.drop('index', axis=1, inplace=True)

df = session(df)

session_shift = df['session'].ne(df['session'].shift()).cumsum()

min_peaks = df.groupby(session_shift)['low'].idxmin()
max_peaks = df.groupby(session_shift)['high'].idxmax()

open_idx = df.groupby(session_shift)['open'].head(1).index
close_idx = df.groupby(session_shift)['close'].tail(1).index

df[['axis', 'peaks']] = None

df.loc[open_idx, 'axis'] = df.loc[open_idx, 'open']
df.loc[close_idx, 'axis'] = df.loc[close_idx, 'close']

df.loc[max_peaks, 'peaks'] = df.loc[max_peaks, 'high']
df.loc[min_peaks, 'peaks'] = df.loc[min_peaks, 'low']

# peaks = df[df['peaks'] != 0]

print(df.head(50))