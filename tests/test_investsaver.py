import pytest
import os
import shutil
import time
import datetime
from os.path import dirname
import logging
import logging.handlers
import investpy
from stockstocker import InvestingSaver


def setLogger(obj):
    if not os.path.exists(dirname(__file__) + "/logs"):
        os.mkdir(dirname(__file__) + "/logs")
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


def test_specify_ymd():

    symbol = "US Dollar Index"
    search_res = investpy.search(text=symbol)
    search_res = [r for r in search_res if r.name == symbol][0]
    # 新規作成
    search_res.retrieve_historical_data(
        from_date='07/30/2020',
        to_date='07/31/2020'
    )


