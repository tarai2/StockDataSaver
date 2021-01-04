import sys
import os
import yaml
import json
import logging
import logging.handlers
import numpy as np
import pandas as pd
import tables
import yfinance as yf
import time
import timeout_decorator
import datetime
import urllib
from glob import glob
from os.path import dirname
from threading import Lock
from sqlalchemy import create_engine

from . import Country, getCountryCode
from . import NumeraiStockUpdater
from .SaverBase import SaverBase
from ..conf.mysqlconf import URL
MYSQLURL = URL + "FinancialData"
DAY1 = datetime.timedelta(1)


class YFinanceSaver(SaverBase):

    def __init__(self):
        super().__init__()

        NumeraiStockUpdater.prepare_config(
            os.path.dirname(__file__) + "/config.yaml"
        )

        # set logger
        self.logger = logging.getLogger(__name__)

        # load config yaml
        with open(os.path.dirname(__file__) + "/config.yaml") as file:
            config_yaml = yaml.load(file)
            self.homedir = config_yaml["homedir"]
            self.config_dict = config_yaml["yfinance"]
            self.logger.info("loaded config.yaml")

        self.permitRetry = True


    def __del__(self):
        super().__del__()


    def push_ohlcv(self, table, symbol):
        """ symbolの1min OHLCVをupdate.
        Args:
            table (str): Daily or Intraday
            symbol (str): e.g. TOYOTA.T
        """
        if table == "Daily":
            interval = "1d"
            period = "max"
        elif table == "Intraday":
            interval = "1m"
            period = "7d"
        else:
            raise ValueError(f"Invalid value of table='{table}'")

        # request
        try:
            self.logger.info(symbol)
            yfTicker = yf.Ticker(symbol)
            latest_date = self._get_latest_date(table, symbol)
            # self.logger.info("fetched latest_date")
            if latest_date is None:
                # new save
                start = None if table == 'Daily' else (datetime.datetime.now()-7*DAY1).strftime("%Y-%m-%d")
                @timeout_decorator.timeout(30)
                def get_data():
                    return yfTicker.history(
                        interval=interval, period=period,
                        start=start,
                        end=(datetime.datetime.now()).strftime("%Y-%m-%d")).drop_duplicates()
                df = get_data()
                self.logger.info(f"New {table} OHLCV '{symbol}'")
            else:
                # update
                if table == 'Intraday' and (datetime.datetime.now() - latest_date) > datetime.timedelta(6):
                    # modify out of range latest_date
                    latest_date = datetime.datetime.now() - 6*DAY1
                if (datetime.datetime.now()-DAY1).date() <= latest_date.date():
                    # no request for invalid range
                    self.logger.info("No updation")
                    return
                @timeout_decorator.timeout(30)
                def get_data():
                    return yfTicker.history(
                        interval=interval,
                        start=latest_date.strftime("%Y-%m-%d"),
                        end=(datetime.datetime.now()-DAY1).strftime("%Y-%m-%d")
                        ).drop_duplicates()
                df = get_data()

            # reform and extract data
            if df.index.tz is None:
                if latest_date is None: latest_date = df.index.min() - datetime.timedelta(seconds=1)
                df = df.iloc[:, :5].loc[latest_date+datetime.timedelta(seconds=1):].reset_index()
            else:
                if latest_date is None: latest_date = df.index.tz_convert(None).min() - datetime.timedelta(seconds=1)
                df = df.tz_convert(None).iloc[:, :5].loc[latest_date+datetime.timedelta(seconds=1):].reset_index()
            df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
            df["ticker"] = symbol

            # push to mysql
            if df.shape[0] > 0:
                con = self.engine.connect()
                df.to_sql(table, con, if_exists="append", index=False)
                self.logger.info(f"{table} OHLCV '{symbol}' pushed")
                con.close()
            # time.sleep(2)
            self.permitRetry = True
            
        except KeyboardInterrupt:
            sys.exit()
        except AttributeError as e:
            self.logger.info(f"Sorry, '{symbol}' seems to have no {table} OHLCV")
        except json.JSONDecodeError as e:
            self.logger.info(f"Sorry, '{symbol}' seems to have no {table} OHLCV")
        except urllib.error.HTTPError as e:
            if self.permitRetry:
                self.logger.info("HTTPError ... Retry")
                self.permitRetry = False
                time.sleep(2)
                self.push_ohlcv(table, symbol)
            else:
                self.logger.info(f"Maximum Retry counts exceeded in HTTPError at '{symbol}'")
        except timeout_decorator.timeout_decorator.TimeoutError as e:
            self.logger.info("Timed Out")
        except Exception as e:
            self.logger.exception(f"Error in downloading Intraday '{symbol}' OHLCV")
            self.logger.exception(e, exc_info=True)


    # def _get_symbol_info(self, symbol, folder_path):
    #     """ symbolのInfoをupdate.
    #     Args:
    #         folder_path (str): e.g. home/Product/Country/Symbol/Info
    #         symbol (str): e.g. TOYOTA.T
    #     """
    #     try:
    #         yfTicker = yf.Ticker(symbol)
    #         latest_date = self._get_latest_date(folder_path, "csv")
    #         # 作成
    #         diff = pd.DataFrame(
    #             [yfTicker.info],
    #             index=pd.DatetimeIndex([datetime.date.today()], name="FetchDate")
    #         )
    #         if latest_date is None:
    #             # 新規作成
    #             if diff.shape[0] > 0:
    #                 diff.to_csv(folder_path + "/" + symbol + ".csv")
    #                 self.logger.info("'{}' Daily INFO was newly saved.".format(symbol))
    #         else:
    #             # append
    #             df = pd.read_csv(folder_path + "/" + symbol + ".csv", index_col=[0], parse_dates=[0])
    #             df.append(diff).to_csv(folder_path + "/" + symbol + ".csv")
    #             self.logger.info("'{}' Daily INFO was updated.".format(symbol))
    #         self.permitRetry = True
    #     except KeyboardInterrupt:
    #         sys.exit()
    #     except json.JSONDecodeError as e:
    #         self.logger.info("Sorry, '{}' seems to have no info")
    #     except IndexError as e:
    #         self.logger.info("Sorry, '{}' seems to have no info".format(symbol))
    #     except ValueError as e:
    #         self.logger.info("Sorry, '{}' seems to have no info".format(symbol))
    #     except KeyError as e:
    #         self.logger.info("Sorry, '{}' seems to have no info".format(symbol))
    #     except urllib.error.HTTPError as e:
    #         if self.permitRetry:
    #             self.logger.info("HTTPError at {} ... Retry".format(symbol))
    #             self.permitRetry = False
    #             time.sleep(2)
    #             self._get_symbol_info(symbol, folder_path)
    #         else:
    #             self.logger.info("Maximum Retry counts exceeded in HTTPError at '{}'".format(symbol))
    #     except Exception as e:
    #         self.logger.exception("Error in Updating Daily INFO '{}'".format(symbol))
    #         self.logger.exception(e, exc_info=True)
