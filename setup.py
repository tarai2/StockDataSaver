from setuptools import setup,find_packages


setup(
    name="stockdatasaver",
    version="0.0.0",
    install_requires=[
        "numpy",
        "pandas",
        "pytest",
        "pytest-mock",
    ],
    packages=find_packages(where='./'),
    package_dir={'': './'},
)


import json
with open('config.json', 'w') as file:
    default_config = {
        "homedir": "./",
        "investing.com": [],
        "yahoo-finance": [], 
    }
    json.dump(default_config, file, indent=4)
