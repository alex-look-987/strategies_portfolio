import os
import numpy as np
import pandas as pd

def data_mgmt(name: str, index: bool, initial_year: int, initial_month: int, end_year: int, end_month: int) -> pd.DataFrame:
    """
    Procesamiento por rango de fechas usando año y mes (YYYY-MM)

    Args:
        name (str): Nombre del archivo CSV (sin '.csv')
        initial (str): Fecha de inicio en formato 'YYYY-MM'
        end (str): Fecha de fin en formato 'YYYY-MM'

    Returns:
        pd.DataFrame: DataFrame filtrado por el rango indicado
    """

    # Cargar y redondear
    df = pd.read_csv(f'{name}.csv', parse_dates=['date'], compression='gzip')
    
    df = df[['date', 'open', 'high', 'low', 'close']].round(5)
    df['date'] = pd.to_datetime(df['date'], format='mixed')

    idx = df[(df['date'].dt.year == initial_year) &
            (df['date'].dt.month == initial_month)].index[0]

    try:
        idx_ = df[(df['date'].dt.year == end_year) &
                (df['date'].dt.month == end_month)].index[-1]
    except IndexError:
        idx_ = len(df)  # Si no encuentra nada, toma hasta el final

    # Indexar y recortar
    if index:
        df['index'] = df.index
    
    df = df.set_index('date')
    df = df[idx:idx_]

    return df

def lags(df: pd.DataFrame, target: str, window_size: int, target_lag: bool) -> pd.DataFrame:
    """
    Lags Feature Management for training models

    This Function Does not Requiere of post shift process for the target. 

    Args:
        df (pd.DataFrame): Dataframe with desired values to Lag
        target (str): Specify Target to do not process lag
        window_size (int): Amount of previous values to take
        target_lag (bool): Wether to create lag for targer or not

    Returns:
        pd.DataFrame: Dataframe with N columns lags from given value
    """

    indicators = df.columns.difference([target]) if not target_lag else df.columns

    lagged = {f'{col}_{i}': df[col].shift(i)
        for col in indicators for i in range(1, window_size + 1)}

    df_lags = pd.DataFrame(lagged, index=df.index)
    df_combined = pd.concat([df, df_lags], axis=1).dropna()

    return df_combined

def lags_custom(df: pd.DataFrame, lag_config: dict[tuple[str, ...], int]) -> pd.DataFrame:
    """
    Crea lags personalizados para grupos de columnas.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        lag_config (dict): Diccionario donde las claves son tuplas de columnas,
                           y los valores son el número de lags.
                           Ejemplo: {('col_1', 'col_2'): 3, ('col_3',): 1}
        keep_original (bool): Si True, mantiene las columnas originales.

    Returns:
        pd.DataFrame: DataFrame con columnas laggeadas.
    """

    lagged = {}

    for cols, n_lags in lag_config.items():
        for col in cols:
            for i in range(1, n_lags + 1):
                lagged[f'{col}_{i}'] = df[col].shift(i)

    df_lags = pd.DataFrame(lagged, index=df.index)
    df_result = pd.concat([df, df_lags], axis=1).dropna()

    return df_result

def adj_range(v, range):
    """
    Ajusta una amplitud `v` al múltiplo más cercano del rango dado, 
    útil para normalizar o categorizar swings de precios en trading.

    Si el valor ajustado es menor a 5 pips en magnitud, se fuerza a ±5 para 
    evitar rangos demasiado pequeños que no sean significativos.

    Args:
        v (float): Valor de la amplitud del swing (puede ser positivo o negativo).
        range (float): Rango base al cual ajustar el valor (ej. múltiplos de 5 o 10 pips).

    Returns:
        float: Valor ajustado al múltiplo más cercano del rango especificado.
    
    Ejemplo:
        >>> adj_range(13, 5)
        15.0
        >>> adj_range(-2, 5)
        -5.0
    """
    
    lower = np.floor(v / range) * range
    upper = np.ceil(v / range) * range
    
    nearest = lower if abs(v - lower) < abs(v - upper) else upper
    
    if abs(nearest) < 5:
        return float(np.sign(v) * 5)
    
    return float(nearest)

def month(number: float) -> str:
    name = {1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril', 5: 'mayo', 6: 'junio', 7: 'julio',
            8: 'agosto', 9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'}
    
    return name.get(number, -1000)

def merge_data(historical: pd.DataFrame, last_week):
   
   df = pd.concat([historical, last_week])

   df.sort_index(ascending=True, inplace=True)
   df = df[~df.index.duplicated(keep='last')]

   return df
    