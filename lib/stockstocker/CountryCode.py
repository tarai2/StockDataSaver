from enum import Enum


from enum import Enum
class Country(Enum):
    US = "US"
    JP = "JP"
    UK = "UK"
    KS = "KS"
    DE = "DE"
    FR = "FR"
    AU = "AU"
    HK = "HK"

    def toYahoo(self):
        if self == Country.US: return ""
        if self == Country.JP: return ".T"
        if self == Country.UK: return ".L"
        if self == Country.KS: return ".KS"
        if self == Country.FR: return ".PA"
        if self == Country.AU: return ".AX"
        if self == Country.DE: return ".DE"

def getCountryCode(country_code):
    """ その他のカントリーコードをMyContryCodeに変換
    Args:
        country_code (str): JT, .T等, NumeraiやYFin使用のカントリーコード
    Returns:
        Country:
    """
    if country_code in ["", "US", "united states", "America"]:
        return Country.US
    elif country_code in ["JT", ".T", "T", "JP", "JPN", "japan"]:
        return Country.JP
    elif country_code in ["LN", ".L", "L", "UK"]:
        return Country.UK
    elif country_code in ["KS", ".KS", "Korea"]:
        return Country.KS
    elif country_code in ["FP", ".PA", "PA", "FR", "france"]:
        return Country.FR
    elif country_code in ["AS", "AU", ".AX", "AX", "AUD"]:
        return Country.AU
    elif country_code in ["GY", "GR", ".DE", "DE", "GER", "germany"]:
        return Country.DE


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
