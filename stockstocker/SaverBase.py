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

Day1 = datetime.timedelta(1)


class SaverBase:

    def __init__(self):
        pass

    @staticmethod
    def _get_latest_date(folder_dir, extention="hdf"):
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

    @staticmethod
    def mkdir(folder_path):
        """ folder_pathに至る全てのpathが存在するか確認し, なければmkdirする
        Args:
            folder_path (str): e.g. ./data/Equity/JP/Intraday
        """
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
