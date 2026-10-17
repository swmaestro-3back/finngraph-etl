# Slot kinds used by the "types" field. These are ontology slot markers, not entity labels:
# the pipeline reads them to decide how a slot is validated (see build_candidate_frames).
#   COMPANY -> anchor slot, must match a gazetteer NER hit
#   PRODUCT -> product slot, free text copied verbatim from the article
# "irreflexive": True marks predicates whose subject and object can never be the same
# company; TripletBuilder drops frames that violate it.
PREDICATE_DICT: dict = {
    # to supply (includes signing a supply contract or winning an order)
    "SUPPLIES_TO": {
        "description": "Supplier supplies or delivers an item to a recipient. This also covers signing a supply contract or winning a supply order/bid (수주) to provide the item — treat those as SUPPLIES_TO as well.",
        "irreflexive": True,
        "arguments": {
            "supplier": {
                "types": ["COMPANY"],
                "description": "Supplier",
                "required": True,
            },
            "recipient": {
                "types": ["COMPANY"],
                "description": "Recipient of the supply",
                "required": True,
            },
            "item": {
                "types": ["PRODUCT"],
                "description": "Product, material or service supplied, copied verbatim from the article",
                "required": True,
            },
        },
    },
    # to invest in
    "INVESTS_IN": {
        "description": "Subject makes a simple capital/equity investment into the object (a minority stake), without taking ownership or control. If the subject obtains ownership or control, use ACQUIRES instead.",
        "irreflexive": True,
        "arguments": {
            "investor": {
                "types": ["COMPANY"],
                "description": "Investor",
                "required": True,
            },
            "investment_recipient": {
                "types": ["COMPANY"],
                "description": "Investment recipient",
                "required": True,
            },
        },
    },
    # to acquire (includes mergers and taking a company as a subsidiary)
    "ACQUIRES": {
        "description": "Subject obtains ownership or control of the object company. This also covers a merger (합병) in which the subject absorbs the object, and the object becoming the subject's subsidiary or affiliate (자회사·계열사 편입) as a result of the deal. For a minority investment without control, use INVESTS_IN instead.",
        "irreflexive": True,
        "arguments": {
            "acquirer": {
                "types": ["COMPANY"],
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
}

REGISTERED_PREDICATES: set[str] = set(PREDICATE_DICT.keys())
