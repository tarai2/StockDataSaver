import sys
import os
import yaml
import json
import logging
import logging.handlers
import numpy as np
import pandas as pd
import yfinance as yf
import time
import datetime
import urllib
from glob import glob
from os.path import dirname
from stockstocker import Country, getCountryCode
from stockstocker.SaverBase import SaverBase
from stockstocker import NumeraiStockUpdater

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

        self.permitRetry = True

    def update_equites(self):
        """ config.yaml内のEquity.Indivisualの一括download
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
            self._get_daily_ohlcv(symbol, folder_path + "Daily")
            # Intraday
            self.mkdir(folder_path + "Intraday")
            self._get_1min_ohlcv(symbol, folder_path + "Intraday")
            # Info
            self.mkdir(folder_path + "Info")
            self._get_symbol_info(symbol, folder_path + "Info")
            time.sleep(1)


    def update_equity_indices(self):
        """ config.yaml内のEquity.Indexの一括download
        """
        indices_dict = self.config_dict["Equity"]["Index"]
        for country_code, symbols in indices_dict.items():
            for symbol in symbols:
                # folder_path作成: home/Equity/Index/JP/N225/
                folder_path = "{}/{}/{}/{}/{}/".format(
                    self.homedir, "Equity", "Index",
                    country_code, symbol
                )
                # Daily
                self.mkdir(folder_path + "Daily")
                self._get_daily_ohlcv(symbol, folder_path + "Daily")
                # Intraday
                self.mkdir(folder_path + "Intraday")
                self._get_1min_ohlcv(symbol, folder_path + "Intraday")
                # Info
                self.mkdir(folder_path + "Info")
                self._get_symbol_info(symbol, folder_path + "Info")
                time.sleep(1)


    def update_commodities(self):
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
                self._get_daily_ohlcv(symbol, folder_path + "Daily")
                # Intraday
                self.mkdir(folder_path + "Intraday")
                self._get_1min_ohlcv(symbol, folder_path + "Intraday")
                # Info
                self.mkdir(folder_path + "Info")
                self._get_symbol_info(symbol, folder_path + "Info")
                time.sleep(1)


    def update_forex(self):
        """ config.yamlのForexの一括download
        """
        tenor_dict = self.config_dict["Forex"]
        for tenor, pair_dict in tenor_dict.items():
            for pair, symbols in pair_dict.items():
                for symbol in symbols:
                    # folder_path作成: home/Forex/Spot/USDJPY/USDJPY=X
                    folder_path = "{}/{}/{}/{}/{}/".format(
                        self.homedir, "Forex", tenor,
                        pair, symbol
                    )
                    # Daily
                    self.mkdir(folder_path + "Daily")
                    self._get_daily_ohlcv(symbol, folder_path + "Daily")
                    # Intraday
                    self.mkdir(folder_path + "Intraday")
                    self._get_1min_ohlcv(symbol, folder_path + "Intraday")
                    time.sleep(1)


    def _get_daily_ohlcv(self, symbol, folder_path):
        """ symbolのDailyOHLCVをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Daily
            symbol (str): e.g. TOYOTA.T
        """
        try:
            yfTicker = yf.Ticker(symbol)
            latest_date = self._get_latest_date(folder_path)
            if latest_date is None:
                # 新規作成
                df = yfTicker.history(period="max", interval="1d")
                if df.shape[0] > 0:
                    df.to_hdf(folder_path + "/" + symbol + ".hdf", key="pandasdf")
                self.logger.info("'{}' Daily OHLCV was newly saved.".format(symbol))
            else:
                # append
                diff = yfTicker\
                    .history(start=(latest_date+Day1).strftime("%Y-%m-%d"),
                             end=datetime.datetime.now().strftime("%Y-%m-%d"))
                df = pd.read_hdf(folder_path + "/" + symbol + ".hdf")
                df.append(diff).to_hdf(folder_path + "/" + symbol + ".hdf", key="pandasdf")
                self.logger.info("'{}' Daily OHLCV was updated.".format(symbol))
            self.permitRetry = True
        except KeyboardInterrupt:
            sys.exit()
        except json.JSONDecodeError as e:
            self.logger.info("Sorry, '{}' seems to have no Daily OHLCV".format(symbol))
        except urllib.error.HTTPError as e:
            if self.permitRetry:
                self.logger.info("HTTPError ... Retry")
                self.permitRetry = False
                time.sleep(2)
                self._get_daily_ohlcv(symbol, folder_path)
            else:
                self.logger.info("Maximum Retry counts exceeded in HTTPError at '{}'".format(symbol))
        except Exception as e:
            self.logger.exception("Error in downloading Daily '{}' OHLCV".format(symbol))
            self.logger.exception(e, exc_info=True)


    def _get_1min_ohlcv(self, symbol, folder_path):
        """ symbolの1min OHLCVをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Intraday
            symbol (str): e.g. TOYOTA.T
        """
        try:
            yfTicker = yf.Ticker(symbol)
            latest_date = self._get_latest_date(folder_path)
            if latest_date is None:
                # 新規作成
                df = yfTicker\
                    .history(period="7d", interval="1m")
                if df.shape[0] > 0:
                    timezone = df.index.tz  # timezone一時保存
                    df.index = df.index.tz_localize(None)
                    for date in np.unique(df.index.date):  # index.dateするとtimezoneが落ちる.
                        df.loc[date:date+Day1]\
                            .tz_localize(timezone)\
                            .to_hdf(folder_path + date.strftime("/%Y-%m-%d.hdf"), key="pandasdf")
                    self.logger.info("'{}' Intraday OHLCV was newly saved.".format(symbol))
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
                self.logger.info("'{}' Intraday OHLCV was updated.".format(symbol))
            self.permitRetry = True
        except KeyboardInterrupt:
            sys.exit()
        except AttributeError as e:
            self.logger.info("Sorry, '{}' seems to have no 1min OHLCV".format(symbol))
        except json.JSONDecodeError as e:
            self.logger.info("Sorry, '{}' seems to have no 1min OHLCV".format(symbol))
        except urllib.error.HTTPError as e:
            if self.permitRetry:
                self.logger.info("HTTPError ... Retry")
                self.permitRetry = False
                time.sleep(2)
                self._get_1min_ohlcv(symbol, folder_path)
            else:
                self.logger.info("Maximum Retry counts exceeded in HTTPError at '{}'".format(symbol))
        except Exception as e:
            self.logger.exception("Error in downloading Intraday '{}' OHLCV".format(symbol))
            self.logger.exception(e, exc_info=True)


    def _get_symbol_info(self, symbol, folder_path):
        """ symbolのInfoをupdate.
        Args:
            folder_path (str): e.g. home/Product/Country/Symbol/Info
            symbol (str): e.g. TOYOTA.T
        """
        try:
            yfTicker = yf.Ticker(symbol)
            latest_date = self._get_latest_date(folder_path, "csv")
            # 作成
            diff = pd.DataFrame(
                [yfTicker.info],
                index=pd.DatetimeIndex([datetime.date.today()], name="FetchDate")
            )
            if latest_date is None:
                # 新規作成
                if diff.shape[0] > 0:
                    diff.to_csv(folder_path + "/" + symbol + ".csv")
                    self.logger.info("'{}' Daily INFO was newly saved.".format(symbol))
            else:
                # append
                df = pd.read_csv(folder_path + "/" + symbol + ".csv")
                df.append(diff).to_csv(folder_path + "/" + symbol + ".csv")
                diff.to_csv(folder_path + "/" + symbol + ".csv")
                self.logger.info("'{}' Daily INFO was updated.".format(symbol))
            self.permitRetry = True
        except KeyboardInterrupt:
            sys.exit()
        except json.JSONDecodeError as e:
            self.logger.info("Sorry, '{}' seems to have no info")
        except IndexError as e:
            self.logger.info("Sorry, '{}' seems to have no info".format(symbol))
        except ValueError as e:
            self.logger.info("Sorry, '{}' seems to have no info".format(symbol))
        except KeyError as e:
            self.logger.info("Sorry, '{}' seems to have no info".format(symbol))
        except urllib.error.HTTPError as e:
            if self.permitRetry:
                self.logger.info("HTTPError at {} ... Retry".format(symbol))
                self.permitRetry = False
                time.sleep(2)
                self._get_symbol_info(symbol, folder_path)
            else:
                self.logger.info("Maximum Retry counts exceeded in HTTPError at '{}'".format(symbol))
        except Exception as e:
            self.logger.exception("Error in Updating Daily INFO '{}'".format(symbol))
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
