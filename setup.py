import json
from glob import glob
from os.path import basename,dirname
from os.path import splitext

from setuptools import setup
from setuptools import find_packages

import src

def _requires_from_file(filename):
    return open(filename).read().splitlines()


setup(
    name="stockstocker",
    version=src.__version__,
    packages=['stockstocker'],  # import可能な名前空間を指定
    package_dir={'stockstocker': 'src'},  # 名前空間とディレクトリsrcの対応
    install_requires=_requires_from_file('requirements.txt')
)

# with open(dirname(__file__)+'/config.json', 'w') as file:
#     default_config = {
#         "homedir": "./",
#         "investing.com": [],
#         "yahoo-finance": [], 
#     }
#     json.dump(default_config, file, indent=4)