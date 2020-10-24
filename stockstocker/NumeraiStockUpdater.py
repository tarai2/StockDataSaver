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
from stockstocker import default_config

from os.path import dirname


class NumeraiStockUpdater:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.prepare_config(dirname(__file__) + "/config.yaml")


    @staticmethod
    def prepare_config(filepath):
        """ config.yamlが存在しない場合, default_config.yamlから生成
        Args:
            filepath (str): ./*/config.yaml
        Effects:
            config.yaml
        """
        if not os.path.exists(filepath):
            with open(filepath, "w") as file:
                yaml.dump(default_config, file, indent=4)


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
            main_stock_ids = self._get_main_stock_id()
            config_yaml["yfinance"]["Equity"]["Indivisual"] =\
                list(set(main_stock_ids + config_yaml["yfinance"]["Equity"]["Indivisual"]))

            # 書込
            with open(dirname(__file__) + "/config.yaml", "w") as file:
                yaml.dump(config_yaml, file, indent=4)
                self.logger.info("successfully updated stock id list.")

        except Exception as e:
            self.logger.exception(e, exc_info=True)


    def _get_main_stock_id(self):
        """ numerai signals予測対象銘柄からメイン地域の銘柄idを抽出し、Yahoo!Financeで使用されている銘柄コードに変換して返す
        Returns:
            list(str): 対象銘柄の配列
        """
        try:
            # download
            res = requests.get(
                "https://numerai-quant-public-data.s3-us-west-2.amazonaws.com/example_predictions/latest.csv"
            )
            df = pd.read_csv(io.StringIO(res.content.decode('utf-8')), header=0, index_col=0)
            self.logger.info("successfully downloaded.")

            # reform
            universe =\
                pd.DataFrame(
                    [val.split(" ") for val in df.index.values],
                    columns=["stock_id", "region"]
                ).fillna("blank")

            # extract & replace region symbol
            main_universe_regions =\
                ["JT", "KS", "LN", "AU", "GY", "FP", "blank"]

            main_universe =\
                universe\
                .query("region in @main_universe_regions")\
                .replace({
                    "blank": "",
                    "JT": ".T",
                    "KS": ".KS",
                    "LN": ".L",
                    "AU": ".AX",
                    "GY": ".DE",
                    "FP": ".PA",
                })
            # USのidの末尾の.を消しておく
            main_universe.stock_id = main_universe.stock_id.apply(lambda x: x.replace(".", ""))

            # 結合することでYahooFinanceIDに直す
            main_stock_ids = main_universe.sum(axis=1).reset_index(drop=True).values.tolist()
            # / をdrop
            main_stock_ids = [_id.replace("/","") for _id in main_stock_ids] 
            return main_stock_ids

        except Exception as e:
            self.logger.exception(e, exc_info=True)


if __name__ == "__main__":

    updater = NumeraiStockUpdater()

    # Logger設定
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
    updater.logger.addHandler(timeHandler)
    updater.logger.setLevel(logging.INFO)

    updater.refresh()
