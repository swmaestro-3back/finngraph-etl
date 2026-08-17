from pipelines.triplets.ontology.gazetteers.commodity_dict import COMMODITY_DICT
from pipelines.triplets.ontology.gazetteers.country_dict import COUNTRY_DICT
from pipelines.triplets.ontology.gazetteers.krx_dict import KRX_COMPANY_DICT
from pipelines.triplets.ontology.gazetteers.product_dict import PRODUCT_DICT
from pipelines.triplets.ontology.gazetteers.us_dict import US_COMPANY_DICT

# KRX_COMPANY와 US_COMPANY는 합쳐서 COMPANY_DICT로 내보냄
COMPANY_DICT = {**KRX_COMPANY_DICT, **US_COMPANY_DICT}

__all__ = ["COUNTRY_DICT", "COMMODITY_DICT", "COMPANY_DICT", "PRODUCT_DICT"]
