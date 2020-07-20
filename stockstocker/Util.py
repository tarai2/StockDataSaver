import sys
import os
import yaml
import logging
import logging.handlers
import numpy as np
import pandas as pd
import time
import datetime
from glob import glob
from os.path import dirname


class Util:

    @staticmethod
    def get_files(filename, daily=False, format="hdf"):
        """ filenameと一致するpath名を持つfileのlistを返す
        Args:
            filename (str): 検索文字列 e.g. US_Dollar_Index 
            daily (bool, optional): Defaults to False.
            format (str, optional): Defaults to "hdf".
        Returns:
            list of str: 
        """
        homedir = Util.get_homedir()
        timehorizon = "Daily" if daily else "Intraday"
        files = sorted(
            glob("{}**/*{}*/**/{}/*.{}".format(homedir, filename, timehorizon, format), recursive=True)
        )
        return files

    @staticmethod
    def change_homedir(folder_path):
        """ データ保存先ディレクトリを変更
        Args:
            folder_path (str): 変更先ディレクトリ 
        """
        with open(dirname(__file__) + "/config.yaml", "w") as file:
            config_dict = yaml.load(file)
            config_dict["homedir"] = folder_path
            yaml.dump(config_dict, file, indent=4)


    @staticmethod
    def get_config():
        """ config.yamlをdictで取得
        Returns:
            dict: 
        """
        with open(dirname(__file__) + "/config.yaml") as file:
            config_dict = yaml.load(file)
        return config_dict


    @staticmethod
    def get_homedir():
        """ データ保存先ディレクトリを取得
        Returns:
            str:
        """
        with open(dirname(__file__) + "/config.yaml") as file:
            config_dict = yaml.load(file)
        return config_dict["homedir"]
