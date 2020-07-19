import sys
import os
import yaml
import logging
import logging.handlers
import numpy as np
import pandas as pd
import yfinance as yf
import time
import datetime
from glob import glob
from os.path import dirname
from stockstocker.SaverBase import SaverBase
import NumeraiStockUpdater

Day1 = datetime.timedelta(1)


class YFinanceSaver(SaverBase):

    def __init__(self):
        NumeraiStockUpdater.prepare_config(
            os.path.dirname(__file__) + "/config.yaml"
        )

        self.logger = logging.getLogger(__name__)
        with open(os.path.dirname(__file__) + "/config.yaml") as file:
            config_yaml = yaml.load(file)
            self.homedir = config_yaml["homedir"]
            self.config_dict = config_yaml["yfinance"]
            self.logger.info("loaded config.yaml")


    def get_equites(self):
        """ config.yaml内のEquityの一括download
        """
        equity_list = self.config_dict["Equity"]["Indivisual"]
        for symbol in equity_list:
            # folder_path作成: home/Equity/Indivisual/JP/TM.T/
            folder_path = "{}/{}/{}/{}/{}/".format(
                self.homedir, "Equity", "Indivisual",
                self._get_equity_country_code(symbol).name, symbol
            )
            # Daily
            self.mkdir(folder_path + "Daily")
            self._get_daily_ohlcv(folder_path + "Daily", symbol)
            # Intraday
            self.mkdir(folder_path + "Intraday")
            self._get_1min_ohlcv(folder_path + "Intraday", symbol)
            # Info
            self.mkdir(folder_path + "Info")
            self._get_symbol_info(folder_path + "Info", symbol)
            time.sleep(1)


    def get_equity_indices(self):
        """ config.yaml内のEquityの一括download
        """
        indices_list = self.config_dict["Equity"]["Index"]
        for country_code, symbols in commodity_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Equity/Index/JP/N225/
                folder_path = "{}/{}/{}/{}/{}/".format(
                    self.homedir, "Equity", "Index",
                    country_code, symbol
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(folder_path + "Daily", symbol)
                # Intraday
                self.mkdir(folder_path + "Intraday")
                self._get_1min_ohlcv(folder_path + "Intraday", symbol)
                # Info
                self.mkdir(folder_path + "Info")
                self._get_symbol_info(folder_path + "Info", symbol)
                time.sleep(1)


    def get_commodities(self):
        """ config.yamlのCommidityの一括download
        """
        commodity_dict = self.config_dict["Commodity"]
        for commodity_product, symbols in commodity_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Commodity/Gold/GC=F/
                folder_path = "{}/{}/{}/{}/".format(
                    self.homedir, "Commodity", commodity_product, symbol
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(folder_path + "Daily", symbol)
                # Intraday
                self.mkdir(folder_path + "Intraday")
                self._get_1min_ohlcv(folder_path + "Intraday", symbol)
                # Info
                self.mkdir(folder_path + "Info")
                self._get_symbol_info(folder_path + "Info", symbol)
                time.sleep(1)


    def get_forex():
        """ config.yamlのForexの一括download
        """
        tenor_dict = self.config_dict["Forex"]
        for currency_pair, symbols in tenor_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Forex/Spot/USDJPY/USDJPY=X
                folder_path = "{}/{}/{}/{}/".format(
                    self.homedir, "Forex", currency_pair, symbol
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(folder_path + "Daily", symbol)
                # Intraday
                self.mkdir(folder_path + "Intraday")
                self._get_1min_ohlcv(folder_path + "Intraday", symbol)
                time.sleep(1)


    def _get_daily_ohlcv(self, symbol, folder_path):
        """ symbolのDailyOHLCVをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Daily
            symbol (str): e.g. TOYOTA.T
        """
        yfTicker = yf.Ticker(symbol)
        latest_date = self._get_latest_date(folder_path)
        try:
            if latest_date is None:
                # 新規作成
                yfTicker\
                    .history(period="max", interval="1d")\
                    .to_hdf(folder_path + "/" + symbol + ".hdf", key="pandasdf")
                self.logger.info("{} Daily OHLCV was saved.".format(symbol))
            else:
                # append
                diff = yfTicker\
                    .history(start=(latest_date+Day1).strftime("%Y-%m-%d"),
                             end=datetime.datetime.now().strftime("%Y-%m-%d"))
                df = pd.read_hdf(folder_path + "/" + symbol + ".hdf")
                df.append(diff).to_hdf(folder_path + "/" + symbol + ".hdf", key="pandasdf")
                self.logger.info("{} Daily OHLCV was updated.".format(symbol))
        except Exception as e:
            self.logger.exception("Error in downloading Daily {} OHLCV".format(symbol))
            self.logger.exception(e, exc_info=True)


    def _get_1min_ohlcv(self, symbol, folder_path):
        """ symbolの1min OHLCVをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Intraday
            symbol (str): e.g. TOYOTA.T
        """
        yfTicker = yf.Ticker(symbol)
        latest_date = self._get_latest_date(folder_path)
        try:
            if latest_date is None:
                # 新規作成
                df = yfTicker\
                    .history(period="7d", interval="1m")
                timezone = df.index.tz  # timezone一時保存
                df.index = df.index.tz_localize(None)
                for date in np.unique(df.index.date):  # index.dateするとtimezoneが落ちる.
                    df.loc[date:date+Day1]\
                        .tz_localize(timezone)\
                        .to_hdf(folder_path + date.strftime("/%Y-%m-%d.hdf"), key="pandasdf")
                self.logger.info("{} Intraday OHLCV was saved.".format(symbol))
            else:
                # update
                diff = yfTicker\
                    .history(interval="1m",
                             start=(latest_date).strftime("%Y-%m-%d"),
                             end=datetime.datetime.now().strftime("%Y-%m-%d"))
                timezone = diff.index.tz
                diff.index = diff.index.tz_localize(None)
                for date in np.unique(diff.index.date):
                    diff.loc[date:date+Day1]\
                        .tz_localize(timezone)\
                        .to_hdf(folder_path + date.strftime("/%Y-%m-%d.hdf"), key="pandasdf")
                self.logger.info("{} Intraday OHLCV was updated.".format(symbol))
        except Exception as e:
            self.logger.exception("Error in downloading Intraday {} OHLCV".format(symbol))
            self.logger.exception(e, exc_info=True)


    def _get_symbol_info(self, symbol, folder_path):
        """ symbolのInfoをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Info
            symbol (str): e.g. TOYOTA.T
        """
        yfTicker = yf.Ticker(symbol)
        latest_date = self._get_latest_date(folder_path, "csv")
        try:
            # 作成
            diff = pd.DataFrame(
                [yfTicker.info],
                index=pd.DatetimeIndex([datetime.date.today()], name="FetchDate")
            )
            if latest_date is None:
                # 新規作成
                diff.to_csv(folder_path + "/" + symbol + ".csv")
                self.logger.info("{} Daily INFO was saved.".format(symbol))
            else:
                # append
                df = pd.read_csv(folder_path + "/" + symbol + ".csv")
                df.append(diff).to_csv(folder_path + "/" + symbol + ".csv")
                diff.to_csv(folder_path + "/" + symbol + ".csv")
                self.logger.info("{} Daily INFO was updated.".format(symbol))
        except Exception as e:
            self.logger.exception("Error in Updating Daily INFO {}".format(symbol))
            self.logger.exception(e, exc_info=True)


    @staticmethod
    def _get_equity_country_code(symbol):
        """ 株式銘柄コードからCountryを判定
        Args:
            symbol (str): e.g. TM.T
        Returns:
            Country:
        """
        symbol_split = symbol.split("/")[-1].split(".")
        if len(symbol_split) > 1:
            return getCountryCode(symbol_split[-1])
        else:
            return Country.US


if __name__ == "__main__":

    ysaver = YFinanceSaver()
