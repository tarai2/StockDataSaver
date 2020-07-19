import pytest
import os
import shutil
import time
import datetime
from os.path import dirname
import logging
import logging.handlers
from stockstocker import YFinanceSaver


def setLogger(obj):
    timeHandler = logging.handlers.TimedRotatingFileHandler(
        filename=dirname(__file__) + '/logs/updater.log',
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



def test_get_daily_ohlcv():
    ysaver = YFinanceSaver()
    setLogger(ysaver)

    symbol = "OCDO.L"
    folder_path = os.path.dirname(__file__) + "/Test/Equity/"

    # Daily
    ysaver.mkdir(folder_path + "Daily")
    ysaver._get_daily_ohlcv(folder_path + "Daily", symbol)

    shutil.rmtree(folder_path)


def test_get_1day_ohlcv():
    ysaver = YFinanceSaver()
    setLogger(ysaver)

    symbol = "OCDO.L"
    folder_path = os.path.dirname(__file__) + "/Test/Equity/"

    # Intraday
    ysaver.mkdir(folder_path + "Intraday")
    ysaver._get_1min_ohlcv(folder_path + "Intraday", symbol)

    shutil.rmtree(folder_path)


def test_get_symbol_info():
    ysaver = YFinanceSaver()
    setLogger(ysaver)

    symbol = "OCDO.L"
    folder_path = os.path.dirname(__file__) + "/Test/Equity/"

    # Info
    ysaver.mkdir(folder_path + "Info")
    ysaver._get_symbol_info(folder_path + "Info", symbol)

    shutil.rmtree(folder_path)
