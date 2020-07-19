import os
from os.path import dirname
import stockstocker
from stockstocker import NumeraiStockUpdater
folder_path = dirname(stockstocker.__file__) + "/"


def test_get_main_stock_id(mocker):
    updater = NumeraiStockUpdater()
    main_stock_id = updater._get_main_stock_id()
    assert len(main_stock_id) > 0


def test_prepare_config():
    updater = NumeraiStockUpdater()
    updater.prepare_config(folder_path + "config.yaml")
    os.remove(folder_path + "config.yaml")


def test_refresh():
    updater = NumeraiStockUpdater()
    updater.refresh()
