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
from stockstocker import NumeraiStockUpdater

Day1 = datetime.timedelta(1)


class InvestingSaver(SaverBase):

    def __init__(self):
        NumeraiStockUpdater.prepare_config(
            os.path.dirname(__file__) + "/config.yaml"
        )

        self.logger = logging.getLogger(__name__)
        with open(os.path.dirname(__file__) + "/config.yaml") as file:
            config_yaml = yaml.load(file)
            self.homedir = config_yaml["homedir"]
            self.config_dict = config_yaml["investing.com"]
            self.logger.info("loaded config.yaml")


    def update_equity_indices(self):
        """ config.yaml内のEquity.Indexを一括Update
        """
        indices_dict = self.config_dict["Equity"]["Index"]
        for country_code, symbols in indices_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Equity/Index/JP/N225/
                folder_path = "{}/{}/{}/{}/{}/".format(
                    self.homedir, "Equity", "Index",
                    country_code, symbol.replace("/","_").replace(" ", "_")
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(symbol, folder_path + "Daily")
                time.sleep(1)


    def update_bonds(self):
        """ config.yaml内のBondsを一括Update
        """
        bonds_dict = self.config_dict["Bond"]
        for country_code, symbols in bonds_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Bond/JP/Japan 10Y/
                folder_path = "{}/{}/{}/{}/".format(
                    self.homedir, "Bond",
                    country_code, symbol.replace(" ", "_")
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(symbol, folder_path + "Daily")
                time.sleep(1)


    def update_currencies(self):
        """ config.yaml内のCurrencyを一括Update
        """
        currency_dict = self.config_dict["Currency"]
        for currency_code, symbols in currency_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Currency/USD/
                folder_path = "{}/{}/{}/{}/".format(
                    self.homedir, "Currency",
                    currency_code, symbol.replace(" ", "_")
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(symbol, folder_path + "Daily")
                time.sleep(1)


    def _get_daily_ohlcv(self, symbol, folder_path):
        """ symbolのDailyOHLCVをupdate.
        Args:
            symbol (str): e.g. US Dollar Index
            folder_path (str): e.g. home/Product/Country/Symbol/Daily
        """
        try:
            latest_date = self._get_latest_date(folder_path)
            search_res = investpy.search(text=symbol)
            search_res = [r for r in search_res if r.name == symbol][0]
            if latest_date is None:
                # 新規作成
                df = search_res.retrieve_historical_data(
                    from_date='01/01/1950',
                    to_date=datetime.date.today().strftime("%d/%m/%Y")
                )
                df.to_hdf(folder_path + "/" + symbol.replace("/","_") + ".hdf", key="pandasdf")
                self.logger.info("'{}' Daily OHLCV was newly saved.".format(symbol))
            else:
                # append
                diff = search_res.retrieve_historical_data(
                    from_date=latest_date.strftime("%d/%m/%Y"),
                    to_date=datetime.date.today().strftime("%d/%m/%Y")
                )
                df = pd.read_hdf(folder_path + "/" + symbol.replace("/","_") + ".hdf")
                df = df.append(diff)
                df.to_hdf(folder_path + "/" + symbol.replace("/","_") + ".hdf", key="pandasdf")
                self.logger.info("'{}' Daily OHLCV was updated.".format(symbol))
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            self.logger.exception("Error in downloading Daily '{}' OHLCV".format(symbol))
            self.logger.exception(e, exc_info=True)
