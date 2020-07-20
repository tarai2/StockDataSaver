import pytest
from stockstocker import Country, getCountryCode
from stockstocker import YFinanceSaver, InvestingSaver


def test_getCountryCode():
    code = getCountryCode("JPN")
    assert code == Country.JP


def test_getCountryCodeFromEquity():
    code = YFinanceSaver._get_equity_country_code("HOGE")
    assert code == Country.US
    code = YFinanceSaver._get_equity_country_code("HOGE.KS")
    assert code == Country.KS
    code = YFinanceSaver._get_equity_country_code("HOGE/FUGA/NYORO/tm.T")
    assert code == Country.JP
