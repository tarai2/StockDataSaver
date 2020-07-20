import pytest
import os
import shutil
import time
import datetime
from os.path import dirname
import logging
import logging.handlers
from stockstocker import Util


def test_get_files():
    files = Util.get_files("N225", True)
    assert len(files) > 0
