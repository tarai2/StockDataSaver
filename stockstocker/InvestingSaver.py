import sys
import os
import yaml
import logging
import logging.handlers
import numpy as np
import pandas as pd
import investpy
import time
import datetime
from glob import glob
from os.path import dirname
from stockstocker.SaverBase import SaverBase

Day1 = datetime.timedelta(1)


class InvestingSaver(SaverBase):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        with open(os.path.dirname(__file__) + "/config.yaml") as file:
            config_yaml = yaml.load(file)
            self.homedir = config_yaml["homedir"]
            self.config_dict = config_yaml["investing.com"]
            self.logger.info("loaded config.yaml")


    def _get_daily_ohlcv(self, symbol, folder_path):
        """ symbolのDailyOHLCVをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Daily
            symbol (str): e.g. US Dollar Index
        """
        latest_date = self._get_latest_date(folder_path)
        search_res = investpy.search(text=symbol)
        search_res = [r for r in search_res if r.name == symbol][0]
        try:
            if latest_date is None:
                # 新規作成
                df = search_res.retrieve_historical_data(
                    from_date='01/01/1950',
                    to_date=datetime.date.today().strftime("%m/%d/%Y")
                )
                df.to_hdf(folder_path + "/" + symbol + ".hdf", key="pandasdf")
                self.logger.info("{} Daily OHLCV was saved.".format(symbol))
            else:
                # append
                diff = search_res.retrieve_historical_data(
                    from_date=latest_date.strftime("%m/%d/%Y"),
                    to_date=datetime.date.today().strftime("%m/%d/%Y")
                )
                df = pd.read_hdf(folder_path + "/" + symbol + ".hdf")
                df.append(diff).to_hdf(folder_path + "/" + symbol + ".hdf", key="pandasdf")
                self.logger.info("{} Daily OHLCV was updated.".format(symbol))
        except Exception as e:
            self.logger.exception("Error in downloading Daily {} OHLCV".format(symbol))
            self.logger.exception(e, exc_info=True)
