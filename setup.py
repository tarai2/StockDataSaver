import subprocess
import json
from glob import glob
from os.path import basename, dirname
from os.path import splitext

from setuptools import setup
from setuptools import find_packages


def take_package_name(name):
    if name.startswith("-e"):
        return name[name.find("=")+1:name.rfind("-")]
    else:
        return name.strip()

def load_requires_from_file(filepath):
    with open(filepath) as fp:
        return [take_package_name(pkg_name) for pkg_name in fp.readlines()]

def load_links_from_file(filepath):
    res = []
    with open(filepath) as fp:
        for pkg_name in fp.readlines():
            if pkg_name.startswith("-e"):
                res.append(pkg_name.split(" ")[1])
    return res

# 無理やりinstall
# subprocess.call("pip install --ignore-installed git+https://github.com/tarai2/investpy.git")

setup(
    name="stockstocker",
    version="0.0.0",
    packages=['stockstocker'],  # import可能な名前空間を指定
    package_dir={'stockstocker': 'stockstocker'},  # 名前空間とディレクトリstockstockerの対応
    install_requires=load_requires_from_file("requirements.txt"),
    dependency_links=load_links_from_file("requirements.txt")
)
