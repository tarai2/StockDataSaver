import time
import datetime
import os
import logging
import logging.handlers
import pandas as pd
from concurrent import futures
from os.path import dirname
from lib.stockstocker import NumeraiStockUpdater, InvestingSaver, YFinanceSaver


def setLogger(obj, logname):
    """ objのloggerにhandlerを持たせる """
    if not os.path.exists('logs'):
        os.mkdir("logs")
    
    # Logger Config
    timeHandler = logging.handlers.TimedRotatingFileHandler(
        filename=f'logs/{logname}.log',
        atTime=datetime.time(0),
        when="MIDNIGHT",
        backupCount=30,
        encoding='utf-8'
    )
    timeHandler.setFormatter(
        logging.Formatter(
            '%(asctime)s.%(msecs)03d,%(levelname)s,'
            '[%(module)s.%(funcName)s.%(name)s L%(lineno)d],%(message)s',
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    obj.logger.addHandler(timeHandler)
    obj.logger.setLevel(logging.INFO)


if __name__ == "__main__":
    numerai = NumeraiStockUpdater()
    yfinance = YFinanceSaver()
    investing = InvestingSaver()
    setLogger(numerai, "numerai")
    setLogger(yfinance, "yahoo")
    setLogger(investing, "investing")

    # 銘柄コードupdate
    # numerai.refresh()

    # @investing.com
    task = []
    sym_inv = pd.json_normalize(investing.config_dict).iloc[0].values.sum()    
    with futures.ThreadPoolExecutor(max_workers=3) as executor1:
        for sym in sym_inv:
            task.append( executor1.submit(investing.push_ohlcv, sym) )
    investing.logger.info("END")


    # @yahoo
    # sym_yf = pd.json_normalize(yfinance.config_dict).iloc[0].values.sum()
    # with futures.ThreadPoolExecutor(max_workers=2) as executor2:
    #     for sym in sym_yf:
    #         task.append( executor2.submit(yfinance.push_ohlcv, "Daily", sym) )
    #         task.append( executor2.submit(yfinance.push_ohlcv, "Intraday", sym) )
    # yfinance.logger.info("END")
    futures.as_completed(fs=task)