import requests
from stockstocker import NumeraiStockUpdater


def test_refresh():
    updater = NumeraiStockUpdater()


def test_get_main_universe(mocker):
    updater = NumeraiStockUpdater()
    main_stock_id = updater.get_main_stock_id()


def test_prepare_config():
    pass
