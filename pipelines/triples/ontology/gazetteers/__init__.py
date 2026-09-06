from .krx_dict import KRX_COMPANY_DICT
from .us_dict import US_COMPANY_DICT

# KRX and US company dicts are merged and exported as one COMPANY_DICT
COMPANY_DICT = {**KRX_COMPANY_DICT, **US_COMPANY_DICT}

__all__ = ["COMPANY_DICT"]
