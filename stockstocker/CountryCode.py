from enum import Enum


class Country(Enum):
    US = "US"
    JP = "JP"
    UK = "UK"
    KS = "KS"
    DE = "DE"
    FR = "FR"
    AU = "AU"

    def toNumerai(self):
        if self == Country.US: return ""
        if self == Country.JP: return "JT"
        if self == Country.UK: return "LN"
        if self == Country.KS: return "KS"
        if self == Country.FR: return "FP"
        if self == Country.AU: return "AS"
        if self == Country.DE: return "GY"

    def toYahoo(self):
        if self == Country.US: return ""
        if self == Country.JP: return ".T"
        if self == Country.UK: return ".L"
        if self == Country.KS: return ".KS"
        if self == Country.FR: return ".PA"
        if self == Country.AU: return ".AX"
        if self == Country.DE: return ".DE"


def CountryCode(country_code):
    if country_code == "": return Country.US
    elif country_code == "JT" or country_code == ".T": return Country.JP
    elif country_code == "LN" or country_code == ".L": return Country.UK
    elif country_code == "KS" or country_code == ".KS": return Country.KS
    elif country_code == "FP" or country_code == ".PA": return Country.FR
    elif country_code == "AS" or country_code == ".AX": return Country.AU
    elif country_code == "GY" or country_code == ".DE": return Country.DE


class CurrencyPair(Enum):
    EURUSD = "EURUSD"
    USDJPY = "USDJPY"
    GBPUSD = "GBPUSD"
    AUDUSD = "AUDUSD"


class Currency(Enum):
    USD = "USD"
    EUR = "EUR"
    JPY = "JPY"
    AUD = "AUD"
