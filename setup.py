from setuptools import setup


setup(
    name="stockdatasaver",
    version="0.0.0",
    install_requires=[
        "numpy",
        "pandas",
        "pytest",
        "pytest-mock",
        "yaml"
    ]
)


import yaml
with open('test_out.yaml', 'w') as file:
    default_config = {
        "homedir": "./",
        "investing.com": [],
        "yahoo-finance": [], 
    }
    yaml.dump(default_config, file, default_flow_style=False)
