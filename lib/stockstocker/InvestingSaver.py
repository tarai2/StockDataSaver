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
from threading import Lock
from sqlalchemy import create_engine

from . import Country, getCountryCode
from . import NumeraiStockUpdater
from .SaverBase import SaverBase
from ..conf.mysqlconf import URL
MYSQLURL = URL + "FinancialData"
DAY1 = datetime.timedelta(1)


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

        self.permitRetry = True
        self._lock = Lock()


    def push_ohlcv(self, symbol):
        """ symbolの1min OHLCVをupdate.
        Args:
            symbol (str): e.g. US Dollar Index
        """
        table = "Daily"
        try:
            with self._lock: self.logger.info(symbol)
            engine = create_engine(MYSQLURL)
            latest_date = self._get_latest_date("Daily", symbol)
            search_res = investpy.search(text=symbol)
            search_res = [r for r in search_res if r.name == symbol][0]
            if latest_date is None:
                # 新規作成
                df = search_res.retrieve_historical_data(
                    from_date='01/01/1950',
                    to_date=datetime.date.today().strftime("%d/%m/%Y")
                ).iloc[:, :5].drop_duplicates()
                with self._lock: self.logger.info(f"New {table} OHLCV '{symbol}'")
            elif latest_date+DAY1 <= datetime.datetime.now():
                # append
                df = search_res.retrieve_historical_data(
                    from_date=(latest_date+DAY1).strftime("%d/%m/%Y"),
                    to_date=datetime.date.today().strftime("%d/%m/%Y")
                ).iloc[:, :5].loc[latest_date+datetime.timedelta(seconds=1):].drop_duplicates()
                with self._lock: self.logger.info(f"{table} OHLCV '{symbol}' pushed")
            else:
                # no request for invalid date range
                engine.dispose()
                return

            # reform and extract 
            df.index.name = "timestamp"
            df.columns = [c.lower() for c in df.columns]
            df["ticker"] = symbol

            # push to mysql
            df.reset_index().to_sql(table, engine, if_exists="append", index=False)
            with self._lock: self.logger.info(f"{table} OHLCV '{symbol}' pushed")
            self.permitRetry = True

        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            with self._lock:
                self.logger.exception(f"Error in downloading {table} '{symbol}' OHLCV")
                self.logger.exception(e, exc_info=True)
        finally:
            engine.dispose()
