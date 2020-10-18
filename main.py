import time
import datetime
import os
import logging
import logging.handlers
from os.path import dirname
from stockstocker import NumeraiStockUpdater, InvestingSaver, YFinanceSaver


def setLogger(obj, logname):
    """ objにloggerを持たせる """
    if not os.path.exists('logs'):
        os.mkdir("logs")
    
    # Logger Config
    timeHandler = logging.handlers.TimedRotatingFileHandler(
        filename=f'logs/{logname}.log',
        atTime=datetime.time(0),
        when="MIDNIGHT",
        backupCount=7,
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
    numerai.refresh()

    # @investing.com
    investing.update_equity_indices()
    investing.update_currencies()
    investing.update_bonds()

    # @Yahoo finance
    yfinance.update_commodities()
    yfinance.update_forex()
    yfinance.update_equity_indices()
    yfinance.update_equites()
