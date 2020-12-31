import sys
import os
import yaml
import logging
import logging.handlers
import numpy as np
import pandas as pd
import tables
import yfinance as yf
import time
import datetime
from glob import glob
from os.path import dirname
from sqlalchemy import create_engine

from .CountryCode import * 
from ..conf.mysqlconf import URL
MYSQLURL = URL + "FinancialData"


class SaverBase:

    @staticmethod
    def __get_latest_date(folder_dir, extention="hdf"):
        """ ディレクトリ内のhdfファイルから, 最も新しいエントリの日時を取得
        Args:
            folder_dir (str): e.g. home/Product/Country/Symbol/Daily
            extention (str): file formatの指定
        Returns:
            None or datetime:
        """
        files = sorted(glob(folder_dir + "/*.{}".format(extention)))
        # ファイルがそもそも存在しない場合はNoneを返す
        if len(files) == 0:
            return None
        
        if extention == "hdf":
            try:
                date = pd.read_hdf(files[-1], start=-1).index[-1].date()
            except OSError:
                # 読み込み不能だった場合にはNoneを返す
                return None
        elif extention == "csv":
            date = pd.read_csv(files[-1], parse_dates=[0], index_col=[0]).index[-1].date()

        if not hasattr(date, "strftime"):
            raise TypeError("{} hasn't DatetimeIndex.".format(folder_dir + "/*.{}".format(extention)))

        return date


    def _get_latest_date(self, table, ticker):
        """ get latest timestamp of target timestamp and ticker
        """
        try:
            engine = create_engine(MYSQLURL)
            con = engine.connect()
            res = con.execute(
                f"select timestamp from {table} where ticker='{ticker}' order by timestamp desc limit 1").first()
            engine.dispose()
            if res is None:
                return None
            else:
                return res[0]

        except Exception as e:
            engine.dispose()
            self.logger.error(e)