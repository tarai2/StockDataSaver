import json
from glob import glob
from os.path import basename,dirname
from os.path import splitext

from setuptools import setup
from setuptools import find_packages


def _requires_from_file(filename):
    return open(filename).read().splitlines()


setup(
    name="stockdatasaver",
    version="0.0.0",
    description="パッケージの説明",
    author="tarai",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    install_requires=_requires_from_file('requirements.txt'),
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)

with open(dirname(__file__)+'/config.json', 'w') as file:
    default_config = {
        "homedir": "./",
        "investing.com": [],
        "yahoo-finance": [], 
    }
    json.dump(default_config, file, indent=4)