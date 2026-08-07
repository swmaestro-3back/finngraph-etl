PREDICATE_DICT: dict = {
    # 매각하다
    "DIVESTS_FROM": {
        "description": "Subject sells or divests its ownership stake in the object company.",
        "arguments": {
            "seller": {
                "types": ["COMPANY", "GOVERNMENT"],
                "description": "Seller",
                "required": True,
            },
            "company_being_sold": {
                "types": ["COMPANY"],
                "description": "Company being sold",
                "required": True,
            },
        },
    },
    # 투자하다
    "INVESTS_IN": {
        "description": "Subject makes a simple capital/equity investment into the object (a minority stake), without taking ownership or control. If the subject obtains ownership or control, use ACQUIRES instead.",
        "arguments": {
            "investor": {
                "types": ["COMPANY", "GOVERNMENT", "COUNTRY"],
                "description": "Investor",
                "required": True,
            },
            "investment_recipient": {
                "types": ["COMPANY", "COUNTRY"],
                "description": "Investment recipient",
                "required": True,
            },
        },
    },
    # 협력하다
    "PARTNERS_WITH": {
        "description": "Subject cooperates with the object on a shared initiative, or signs a formal agreement/partnership with the object.",
        "arguments": {
            "cooperating_party": {
                "types": ["COMPANY", "GOVERNMENT", "COUNTRY"],
                "description": "Cooperating party",
                "required": True,
            },
            "cooperation_partner": {
                "types": ["COMPANY", "GOVERNMENT", "COUNTRY"],
                "description": "Cooperation partner",
                "required": True,
            },
        },
    },
    # 인수하다 (합병·자회사 편입 포함)
    "ACQUIRES": {
        "description": "Subject obtains ownership or control of the object company. This also covers a merger (합병) in which the subject absorbs the object, and the object becoming the subject's subsidiary or affiliate (자회사·계열사 편입) as a result of the deal. For a minority investment without control, use INVESTS_IN instead.",
        "arguments": {
            "acquirer": {
                "types": ["COMPANY", "GOVERNMENT"],
                "description": "Acquirer",
                "required": True,
            },
            "company_being_acquired": {
                "types": ["COMPANY"],
                "description": "Company being acquired",
                "required": True,
            },
        },
    },
    # 공급하다 (공급계약 체결·수주 포함)
    "SUPPLIES_TO": {
        "description": "Supplier supplies or delivers an item to a recipient. This also covers signing a supply contract or winning a supply order/bid (수주) to provide the item — treat those as SUPPLIES_TO as well.",
        "arguments": {
            "supplier": {"types": ["COMPANY"], "description": "Supplier", "required": True},
            "recipient": {
                "types": ["COMPANY", "GOVERNMENT", "COUNTRY"],
                "description": "Recipient of the supply",
                "required": True,
            },
            "item": {
                "types": ["COMMODITY", "PRODUCT"],
                "description": "Product or material supplied",
                "required": True,
            },
        },
    },
    # 수출하다
    "EXPORTS_TO": {
        "description": "Exporter exports an item to an importing party abroad. Extracted as a single event. If a sentence describes importing (A imports X from B), restate it in this direction (B exports X to A) instead of a separate import predicate.",
        "arguments": {
            "exporter": {
                "types": ["COMPANY", "COUNTRY"],
                "description": "Exporter",
                "required": True,
            },
            "importing_party": {
                "types": ["COMPANY", "COUNTRY"],
                "description": "Importing party",
                "required": True,
            },
            "item": {
                "types": ["COMMODITY", "PRODUCT"],
                "description": "Product or commodity exported",
                "required": True,
            },
        },
    },
    # 위치하다
    "LOCATED_IN": {
        "description": "Subject is located or headquartered in the object country.",
        "arguments": {
            "located_entity": {
                "types": ["COMPANY"],
                "description": "Located entity",
                "required": True,
            },
            "location": {
                "types": ["COUNTRY"],
                "description": "Location (country)",
                "required": True,
            },
        },
    },
    # 생산하다
    "PRODUCES": {
        "description": "Subject manufactures or mass-produces the object. Use DEVELOPS for the R&D/development stage before mass production.",
        "arguments": {
            "producer": {"types": ["COMPANY"], "description": "Producer", "required": True},
            "produced_item": {
                "types": ["PRODUCT", "COMMODITY"],
                "description": "Product or commodity produced",
                "required": True,
            },
        },
    },
    # 경쟁하다
    "COMPETES_WITH": {
        "description": "Subject competes with the object company in a product or market. Symmetric relationship. Note that two companies may both compete and cooperate in different areas, so this does not preclude PARTNERS_WITH.",
        "arguments": {
            "competing_company": {
                "types": ["COMPANY"],
                "description": "One competing company",
                "required": True,
            },
            "competitor": {
                "types": ["COMPANY"],
                "description": "The rival company",
                "required": True,
            },
        },
    },
    # 개발하다
    "DEVELOPS": {
        "description": "Subject develops or is developing the object as a product/material. Use this for the R&D or development stage (e.g. '개발 중', '개발 완료'), as distinct from actual mass production (use PRODUCES for that).",
        "arguments": {
            "developer": {"types": ["COMPANY"], "description": "Developer", "required": True},
            "developed_item": {
                "types": ["PRODUCT", "COMMODITY"],
                "description": "Product or material being developed",
                "required": True,
            },
        },
    },
    # 제재·규제하다
    "SANCTIONS": {
        "description": "Subject imposes sanctions, export controls, or regulatory restrictions on the object. Typically a government or country acting against another country or a company.",
        "arguments": {
            "sanctioning_party": {
                "types": ["GOVERNMENT", "COUNTRY"],
                "description": "Party imposing the sanction or restriction",
                "required": True,
            },
            "sanctioned_party": {
                "types": ["COUNTRY", "COMPANY"],
                "description": "Party being sanctioned or restricted",
                "required": True,
            },
        },
    },
}
