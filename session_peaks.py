import numpy as np
import pandas as pd
from miscs.utils import data_mgmt
from features.features import sessions
from features.fractals import fractal_axis

df = data_mgmt('datasets/eurusd_m15_historical', False, 2025, 2, 2025, 2)

df = sessions(df)
df = fractal_axis(df)

print(df)   