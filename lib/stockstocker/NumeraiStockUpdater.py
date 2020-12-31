import sys
import os
import io
import yaml
import requests
import logging
import logging.handlers
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from os.path import dirname

from . import default_config
from .CountryCode import * 
from ..conf.mysqlconf import URL
MYSQLURL = URL + "FinancialData"


class NumeraiStockUpdater:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.prepare_config(dirname(__file__) + "/config.yaml")

    @staticmethod
    def prepare_config(filepath):
        """ config.yamlが存在しない場合, default_configからconfig.yamlを生成
        Args:
            filepath (str): ./*/config.yaml
        Effects:
            config.yaml
        """
        if not os.path.exists(filepath):
            with open(filepath, "w") as file:
                yaml.dump(default_config, file, indent=4)


    def push_to_mysql(self):
        """ load config.yaml and write cotents to mysql
        """
        # load yaml
        with open(dirname(__file__) + "/config.yaml") as file:
            config_yaml = yaml.load(file)
        
        # parse to DataFrame
        res = pd.json_normalize(config_yaml, sep="¥").iloc[0]
        df_lst = []
        for k,lst in res.items():
            if k != "homedir":
                a = pd.DataFrame(lst, columns=["ticker"])
                for i,att in enumerate(k.split("¥")):
                    a[i] = att
                df_lst.append(a)

        stock_table = pd.concat(df_lst).reset_index(drop=True).drop(0, axis=1)
        stock_table.columns = ["ticker", "category1", "category2", "country"]

        # push to mysql
        try:
            engine = create_engine(MYSQLURL)
            stock_table.to_sql("TickerID", engine, if_exists='replace', index=False)
            engine.dispose()
        except Exception as e:
            self.logger.error(e)
            engine.dispose()


    def refresh(self):
        """ 同ディレクトリにあるconfigのyfinance項目を更新
        Effects:
            config.yaml
        """
        try:
            # 読込
            with open(dirname(__file__) + "/config.yaml") as file:
                config_yaml = yaml.load(file)

            # numeraiから追加銘柄取得
            stock_id = self._get_main_stock_id()
            for region in stock_id.region.unique():
                config_yaml["yfinance"]["Equity"]["Indivisual"][region] =\
                    stock_id.query("region == @region").stock_id_yf.values.tolist()

            # 書込
            with open(dirname(__file__) + "/config.yaml", "w") as file:
                yaml.dump(config_yaml, file, indent=4)
                self.logger.info("successfully updated stock id list.")

        except Exception as e:
            self.logger.exception(e, exc_info=True)


    def _get_main_stock_id(self):
        """ numerai signals予測対象銘柄からメイン地域の銘柄idを抽出し、YahooFinanceで使用されている銘柄コードに変換して返す
        Returns:
            pd.DataFrame: [stock_id, region, region_yf, stock_id_yf]
        """
        try:
            # download
            res = requests.get(
                "https://numerai-quant-public-data.s3-us-west-2.amazonaws.com/example_predictions/latest.csv"
            )
            df = pd.read_csv(io.StringIO(res.content.decode('utf-8')), header=0, index_col=0)
            stock_ids = np.unique(df.index)
            self.logger.info("successfully downloaded.")

            # reform
            universe =\
                pd.DataFrame(
                    [val.split(" ") for val in stock_ids],
                    columns=["stock_id", "region"]
                )
            universe.stock_id = [_id.replace("/", "") for _id in universe.stock_id]

            # extract target region symbol
            main_universe_regions = ["US", "JP", "AU", "LN", "FP", "GR"]
            main_universe = universe.query("region in @main_universe_regions")

            # numerai region -> my region
            main_universe["region"] = main_universe.region.apply(lambda x: getCountryCode(x).value)

            # my region -> yf region
            main_universe["region_yf"] = main_universe.region.apply(lambda x: getCountryCode(x).toYahoo())
            main_universe = main_universe.reset_index(drop=True)

            # get region id in yfinance
            main_universe["stock_id_yf"] = main_universe[["stock_id", "region_yf"]].sum(axis=1).values

            return main_universe

        except Exception as e:
            self.logger.exception(e, exc_info=True)
