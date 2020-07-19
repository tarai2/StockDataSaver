import pytest
import os
import shutil
import time
import datetime
from os.path import dirname
import logging
import logging.handlers
from stockstocker import InvestingSaver


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



def test_get_1day_ohlcv():
    isaver = InvestingSaver()
    setLogger(isaver)

    symbol = "US Dollar Index"
    folder_path = os.path.dirname(__file__) + "/Test/Currency/"
    # Daily
    isaver.mkdir(folder_path + "Daily")
    isaver._get_daily_ohlcv(symbol, folder_path + "Daily")

    shutil.rmtree(folder_path)
