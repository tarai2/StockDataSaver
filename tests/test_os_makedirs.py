import os
import pytest
from stockstocker import YFinanceSaver


def test_makedirs():
    dirname = "./hoge/fuga/"
    os.makedirs(dirname)
    assert os.path.exists(dirname)
    dirname = "./hoge/fuga/piyo"
    os.makedirs(dirname)
    assert os.path.exists(dirname)
    os.removedirs(dirname)
