import numpy as np
import pandas as pd

def fractal_axis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Obtención ejes para creación de fractales por sesión
    Debe incluir columna clasificación de zonas horarias: ver features

    - Apertura sesión:  open 
    - Pico Máximo: high
    - Píco Mínimo: low
    - Cerre sesión: close
    """

    session_shift = df['session'].ne(df['session'].shift()).cumsum()
    groups = df.groupby(session_shift)

    min_peaks = groups['low'].idxmin()
    max_peaks = groups['high'].idxmax()

    open_idx = groups['open'].head(1).index
    close_idx = groups['close'].tail(1).index

    df[['axis', 'peaks']] = None

    ohlc = ['open', 'close', 'high', 'low']
    mapping_cols = ['axis', 'axis', 'peaks', 'peaks']
    mapping_values = [open_idx, close_idx, max_peaks, min_peaks]
    
    for idx, src, col in zip(mapping_values, ohlc, mapping_cols):
        df.loc[idx, col] = df.loc[idx, src]

    return df