import numpy as np
import pandas as pd
from itertools import product

''' functions_to_apply = {
    'mean':       False,
    'std':        False,
    'sum':        False,
    'max':        False,
    'min':        False,
    'median':     False,
    'skew':       False,
    'diff':       True,
    'pct_change': True} '''

agg_funcs = {
    'mean': lambda s, w: pd.Series(s).rolling(w).mean(),
    'std': lambda s, w: pd.Series(s).rolling(w).std(),
    'sum': lambda s, w: pd.Series(s).rolling(w).sum(),
    'max': lambda s, w: pd.Series(s).rolling(w).max(),
    'min': lambda s, w: pd.Series(s).rolling(w).min(),
    'median': lambda s, w: pd.Series(s).rolling(w).median(),
    'skew': lambda s, w: pd.Series(s).rolling(w).skew(),
    'diff': lambda s, w: pd.Series(s).diff(w-1),
    'pct_change': lambda s, w: pd.Series(s).pct_change(w)}

extractors_date = {
        'hour':      lambda d: pd.Series(d).dt.hour,      # hora de 0 a 23
        'minute':    lambda d: pd.Series(d).dt.minute,    # minuto: 0, 15, 30, 45
        'dayofweek': lambda d: pd.Series(d).dt.dayofweek, # 0=Lunes, 6=Domingo
        'month':     lambda d: pd.Series(d).dt.month,     # 1 a 12
        'day':       lambda d: pd.Series(d).dt.day,       # día del mes
        'week':      lambda d: pd.Series(d).dt.isocalendar().week.astype(int),  # semana ISO
        'year':       lambda d: pd.Series(d).dt.year}       # día del mes

def computation(df: pd.DataFrame, functions_to_apply: dict, window: list, feature: list, class_: bool) -> pd.DataFrame:
    """
    Función para el procesamiento general de funciones a promedios móviles como:
    - mean
    - std
    - sum
    - max
    - min
    - median

    Args:
        df (pd.DataFrame): Dataframe con la información a procesar
        functions_to_apply (dict): Diccionario con valor booleano de las funcione
        window (list): Cantidad de diferentes ventanas: [3, 5, 8]
        feature (list): Total de valores a generar: ["high", "low"]
        class (bool): Clasificación de diff y pct_change: 1 subida, -1 bajada, 0 sin cambio

    Returns:
        pd.DataFrame: Dataframe con el siguiente tipo de columnas:
        - feature_func_window: high_mean_3
    """    
    
    active_funcs = {k: v for k, v in agg_funcs.items() if functions_to_apply.get(k, False)}

    for window, feature, func_name in product(window, feature, active_funcs):
        if feature in {'type'} and func_name in {'diff', 'pct_change'}:
            continue

        col_name = f'{feature}_{func_name}_{window}'
        df[col_name] = agg_funcs[func_name](df[feature], window)

        if class_ and func_name in {'diff', 'pct_change'}:
            df[f'{col_name}_class'] = np.where(df[col_name] > 0, 1, -1)

    return df

def date_feature(df: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    """
    Procesamiento de valores tipo Datetime como característica
    - minute
    - hour
    - day
    - dayofweek
    - week
    - month
    - year

    Args:
        df (pd.DataFrame): Dataframe con columna date
        dates (list[str]): valores datetime a generar

    Returns:
        pd.DataFrame: Dataframe con las columnas resultantes
    """    
    
    df['date'] = pd.to_datetime(df.index, format='mixed')

    for feature in dates:
        if feature in extractors_date:
            df[feature] = extractors_date[feature](df['date'])

    df.set_index('date', inplace=True)
    
    return df

def in_range(t, start, end):
    """Verifica si la hora t (float) está en el rango horario, incluso si cruza medianoche."""
    if start < end:
        return (t >= start) & (t < end)
    else:
        return (t >= start) | (t < end)

def sessions(df: pd.DataFrame):
    """
    Clasifica la sesión de mercado (en UTC) según la hora del timestamp.

    Sesiones (UTC):
        - Sydney:     17:00 - 02:00
        - Tokyo:      20:00 - 05:00
        - London:     04:00 - 13:00
        - New York:   08:00 - 17:00

    Overlaps:
        - Sydney-Tokyo: 20:00 - 02:00
        - Tokyo-London: 04:00 - 05:00
        - London-NewYork: 08:00 - 13:00
    """

    time = df.index.hour + df.index.minute / 60

    conditions = [
        in_range(time, 20, 2),   # overlap sydney-tokyo
        in_range(time, 4, 5),    # overlap tokyo-london
        in_range(time, 8, 13),   # overlap london-ny
        in_range(time, 17, 2),   # sydney
        in_range(time, 20, 5),   # tokyo
        in_range(time, 4, 13),   # london
        in_range(time, 8, 17) ]   # new york

    choices = [
        "overlap_sy_ty",
        "overlap_ty_ln",
        "overlap_ln_ny",
        "sydney",
        "tokyo",
        "london",
        "new_york",]

    df['session'] = np.select(conditions, choices, default='none')
    
    return df
    
def session(df: pd.DataFrame):
    all_sessions = ['sydney', 'tokyo', 'london', 'overlap_london_ny', 'new_york']

    df['session'] = df.index.to_series().apply(sessions)
    
    dummies = pd.get_dummies(df['session'], dtype=int).reindex(columns=all_sessions, fill_value=0)
    
    df = pd.concat([df, dummies], axis=1)
    df.drop(columns='session', inplace=True)
    
    return df
