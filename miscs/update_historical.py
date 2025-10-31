import pandas as pd
import threading, time
from typing import Dict
from itertools import product
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from miscs.ibapi_utils import contracts, RequestData

class TestApp(EClient, EWrapper):
    def __init__(self, symbols=None, frames=None):
        EClient.__init__(self, self)
        self.reqIdCounter = 1
        self.df: Dict[str, pd.DataFrame] = {}
        self.data: Dict[int, RequestData] = {}
        self.symbols = symbols
        self.frames = frames

    def request_historical(self, symbol: str, duration: str, bar_size: str, alias: str):
        self.reqHistoricalData(
            self.reqIdCounter,
            contracts(symbol)[0],
            "", duration, bar_size, "MIDPOINT",
            0, 1, False, [])
        
        self.data[self.reqIdCounter] = RequestData(symbol, alias)
        self.reqIdCounter += 1

    def nextValidId(self, reqId):
        for symbol, (duration, alias) in product(self.symbols, self.frames.items()):
            self.request_historical(symbol, "2 W", duration, alias)

    def historicalData(self, reqId, bar):
        self.data[reqId].bars.append({
            "date": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close})

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        request = self.data[reqId]
        key = f"{request.symbol}_{request.time_frame}".lower()
        df = pd.DataFrame(request.bars)
        
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d %H:%M:%S %Z", errors="coerce")
        df["date"] = df["date"].dt.tz_localize(None)
        
        df.set_index("date", inplace=True)
        self.df[key] = df.round(5)

def fetch_historical_data(symbols, frames):
    
    app = TestApp(symbols=symbols, frames=frames)
    app.connect("127.0.0.1", 7496, 1000)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    # Espera a que los datos estén completos (simplificado, puedes usar un Event más robusto)
    while not app.df:  # Espera hasta que al menos un df esté listo
        time.sleep(0.5)

    # Una vez que los datos están listos, desconectamos
    app.disconnect()
    api_thread.join(timeout=1)

    return app
