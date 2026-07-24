import asyncio
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any

import httpx

from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)

MIN_ARTICLE_BODY_CHARS = 40
NOISE_DOMINATED_BODY_MAX_CHARS = 240
NOISE_DOMINATED_RATIO = 0.65
MATERIAL_EVENT_PROMPT_DIRECTORY = Path(__file__).with_name("prompts")
MARKET_INDEX_PATTERN = re.compile(
    r"(?:코스피|KOSPI|코스닥|KOSDAQ|유가증권시장|주식시장|국내\s*증시|"
    r"증시|나스닥|NASDAQ|다우(?:존스)?|DOW|S\s*&\s*P\s*500|"
    r"닛케이|NIKKEI|상하이\s*종합|항셍)",
    re.IGNORECASE,
)
MARKET_MOVEMENT_PATTERN = re.compile(
    r"(?:상승|하락|급등|급락|강세|약세|오름세|내림세|보합|혼조|"
    r"반등|반락|상승세|하락세|올랐|내렸|마감|출발|개장|장중|"
    r"포인트|퍼센트|%|최고치|최저치)",
    re.IGNORECASE,
)
MARKET_OBSERVATION_PATTERN = re.compile(
    r"(?:순매수|순매도|매수\s*우위|매도\s*우위|외국인|기관|개인|"
    r"거래량|거래대금|시가총액|시총|투자심리|차익\s*실현|"
    r"상승\s*종목|하락\s*종목|환율|원[·/]달러)",
    re.IGNORECASE,
)
VERIFIED_NON_PRICE_EVENT_PATTERN = re.compile(
    r"(?:계약을?\s*(?:체결|해지|종료)했|수주를?\s*(?:확정|공시)했|"
    r"실적을?\s*(?:발표|공시)했|"
    r"(?:영업이익|순이익|매출액)(?:은|이|가|을)?[^.!?。]{0,40}"
    r"(?:기록|증가|감소|확정|집계|발표|공시)(?:했|됐)|"
    r"정책을?\s*(?:시행|확정|의결)했|법안이?\s*(?:통과|시행)됐|"
    r"(?:허가|승인|인가|제재|처분|명령)을?\s*(?:확정|결정|발표|내렸)|"
    r"리콜을?\s*(?:결정|시행)했|사고가?\s*발생했|"
    r"(?:생산|가동|판매|운항)이?\s*(?:중단|재개)됐|"
    r"(?:인수|합병|투자|증자|감자)를?\s*(?:완료|확정|결정)했|"
    r"(?:판결|선고|공시|파산|부도|상장폐지)가?\s*(?:확정|발표|결정|이뤄졌)|"
    r"reported\s+(?:earnings|revenue)|signed\s+(?:a\s+)?contract|"
    r"final\s+(?:order|ruling|approval)|completed\s+(?:the\s+)?acquisition)",
    re.IGNORECASE,
)
MATERIAL_RELATION_SCHEMA: dict[str, dict[str, tuple[str, ...]]] = {
    "인수하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY",)},
    "설립하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY",)},
    "합병하다": {"subject": ("COMPANY",), "object": ("COMPANY",)},
    "매각하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY",)},
    "투자하다": {"subject": ("COMPANY", "GOVERNMENT", "COUNTRY"), "object": ("COMPANY", "COUNTRY")},
    "계약하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY", "GOVERNMENT")},
    "분할하다": {"subject": ("COMPANY",), "object": ("COMPANY",)},
    "제휴하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY", "GOVERNMENT")},
    "수주하다": {
        "subject": ("COMPANY",),
        "object": ("COMPANY", "GOVERNMENT", "PRODUCT", "COMMODITY"),
    },
    "선정하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY",)},
    "취득하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY",)},
    "매입하다": {"subject": ("COMPANY", "GOVERNMENT"), "object": ("COMPANY",)},
    "분사하다": {"subject": ("COMPANY",), "object": ("COMPANY",)},
    "협력하다": {
        "subject": ("COMPANY", "GOVERNMENT", "COUNTRY"),
        "object": ("COMPANY", "GOVERNMENT", "COUNTRY"),
    },
    "유치하다": {"subject": ("COMPANY", "GOVERNMENT", "COUNTRY"), "object": ("COMPANY",)},
    "체결하다": {
        "subject": ("COMPANY", "GOVERNMENT", "COUNTRY"),
        "object": ("COMPANY", "GOVERNMENT", "COUNTRY"),
    },
    "낙찰받다": {"subject": ("COMPANY",), "object": ("COMPANY", "GOVERNMENT")},
    "입찰하다": {"subject": ("COMPANY",), "object": ("COMPANY", "GOVERNMENT")},
    "공급하다": {
        "subject": ("COMPANY",),
        "object": ("COMPANY", "GOVERNMENT", "COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "공급받다": {
        "subject": ("COMPANY", "GOVERNMENT", "COUNTRY"),
        "object": ("COMPANY", "COMMODITY", "PRODUCT"),
    },
    "납품하다": {
        "subject": ("COMPANY",),
        "object": ("COMPANY", "GOVERNMENT", "COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "제공하다": {
        "subject": ("COMPANY", "GOVERNMENT"),
        "object": ("COMPANY", "GOVERNMENT", "COMMODITY", "PRODUCT"),
    },
    "판매하다": {
        "subject": ("COMPANY",),
        "object": ("COMPANY", "GOVERNMENT", "COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "구매하다": {
        "subject": ("COMPANY", "GOVERNMENT", "COUNTRY"),
        "object": ("COMPANY", "COMMODITY", "PRODUCT"),
    },
    "조달하다": {
        "subject": ("COMPANY", "GOVERNMENT"),
        "object": ("COMPANY", "COMMODITY", "PRODUCT"),
    },
    "위탁하다": {
        "subject": ("COMPANY", "GOVERNMENT"),
        "object": ("COMPANY", "COMMODITY", "PRODUCT"),
    },
    "유통하다": {"subject": ("COMPANY",), "object": ("COMPANY", "COMMODITY", "PRODUCT")},
    "의존하다": {
        "subject": ("COMPANY", "GOVERNMENT", "COUNTRY"),
        "object": ("COMPANY", "COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "수출하다": {
        "subject": ("COMPANY", "COUNTRY"),
        "object": ("COMPANY", "COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "수입하다": {
        "subject": ("COMPANY", "COUNTRY"),
        "object": ("COMPANY", "COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "생산하다": {"subject": ("COMPANY", "COUNTRY"), "object": ("COMMODITY", "PRODUCT")},
    "증산하다": {"subject": ("COMPANY", "COUNTRY"), "object": ("COMMODITY", "PRODUCT")},
    "감산하다": {"subject": ("COMPANY", "COUNTRY"), "object": ("COMMODITY", "PRODUCT")},
    "채굴하다": {"subject": ("COMPANY", "COUNTRY"), "object": ("COMMODITY",)},
    "제재하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "GOVERNMENT", "COMPANY"),
    },
    "규제하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "승인하다": {"subject": ("GOVERNMENT",), "object": ("COMPANY", "PRODUCT")},
    "금지하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "제한하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "관세를 부과하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "COMMODITY", "PRODUCT"),
    },
    "수출을 금지하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "수입을 금지하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "협정을 체결하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COUNTRY", "GOVERNMENT")},
    "협정을 파기하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COUNTRY", "GOVERNMENT")},
    "국교를 단절하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COUNTRY", "GOVERNMENT")},
    "동맹하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COUNTRY", "GOVERNMENT")},
    "공격하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "GOVERNMENT", "COMPANY"),
    },
    "침공하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COUNTRY", "GOVERNMENT")},
    "봉쇄하다": {
        "subject": ("COUNTRY", "GOVERNMENT"),
        "object": ("COUNTRY", "GOVERNMENT", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "휴전하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COUNTRY", "GOVERNMENT")},
    "지원하다": {
        "subject": ("COUNTRY", "GOVERNMENT", "COMPANY"),
        "object": ("COUNTRY", "GOVERNMENT", "COMPANY", "COMMODITY", "PRODUCT"),
    },
    "대출하다": {
        "subject": ("GOVERNMENT", "COMPANY"),
        "object": ("COUNTRY", "GOVERNMENT", "COMPANY"),
    },
    "소송하다": {
        "subject": ("COUNTRY", "GOVERNMENT", "COMPANY"),
        "object": ("COUNTRY", "GOVERNMENT", "COMPANY"),
    },
    "판결하다": {"subject": ("GOVERNMENT",), "object": ("COUNTRY", "GOVERNMENT", "COMPANY")},
    "국유화하다": {"subject": ("COUNTRY", "GOVERNMENT"), "object": ("COMPANY", "COMMODITY")},
}
MATERIAL_IMPACT_CHANNELS = {
    "REVENUE",
    "COST",
    "PROFIT",
    "CASH_FLOW",
    "ASSET",
    "FINANCING",
    "OWNERSHIP",
    "PRODUCTION",
    "SUPPLY",
    "DEMAND",
    "CAPACITY",
    "OPERATIONS",
    "MARKET_ACCESS",
    "REGULATION",
    "TRADE",
    "MACRO",
}
MATERIAL_RELATION_SURFACE_PATTERN = re.compile(
    r"(?:인수했|인수한다|설립했|합병했|매각했|투자했|계약했|계약을?\s*체결|"
    r"제휴했|수주했|수주를?\s*(?:확정|공시)|선정했|선정됐|선택했|선택을?\s*받|"
    r"(?:사업자|운영사|공급사)로\s*선정|(?:물량|선로|사업권|공급권)을?\s*확보|"
    r"취득했|매입했|분사했|협력했|유치했|낙찰|입찰했|"
    r"공급했|공급받|납품했|판매했|구매했|조달했|위탁했|유통했|의존하|"
    r"수출했|수입했|생산했|증산했|감산했|채굴했|제재했|규제했|승인했|"
    r"금지했|제한했|관세를?\s*부과|협정을?\s*(?:체결|파기)|"
    r"국교를?\s*단절|동맹을?\s*체결|공격했|침공했|봉쇄했|휴전했|"
    r"지원했|대출했|소송했|판결했|국유화했)",
    re.IGNORECASE,
)


def get_material_event_filter_config() -> dict[str, Any]:
    try:
        from pipelines.news import config

        return {
            "enable_llm": True,
            "provider": getattr(config, "MATERIAL_EVENT_FILTER_PROVIDER", "vllm"),
            "body_limit": getattr(config, "MATERIAL_EVENT_FILTER_BODY_LIMIT", 12000),
            "max_tokens": getattr(config, "MATERIAL_EVENT_FILTER_MAX_TOKENS", 80),
            "max_workers": getattr(config, "MATERIAL_EVENT_FILTER_MAX_WORKERS", 3),
            "max_concurrency": getattr(
                config,
                "MATERIAL_EVENT_FILTER_MAX_CONCURRENCY",
                3,
            ),
            "batch_size": getattr(config, "MATERIAL_EVENT_FILTER_BATCH_SIZE", 3),
            "fail_open": getattr(config, "MATERIAL_EVENT_FILTER_FAIL_OPEN", False),
            "vllm_base_url": getattr(config, "VLLM_BASE_URL", ""),
            "vllm_model": getattr(config, "VLLM_CHAT_MODEL", ""),
            "vllm_api_key": getattr(config, "VLLM_API_KEY", "EMPTY"),
            "vllm_timeout": getattr(config, "VLLM_REQUEST_TIMEOUT", 300),
            "cloud_base_url": getattr(config, "CLOUD_LLM_BASE_URL", ""),
            "cloud_model": getattr(config, "CLOUD_LLM_CHAT_MODEL", ""),
            "cloud_api_key": getattr(config, "CLOUD_LLM_API_KEY", ""),
            "cloud_timeout": getattr(config, "CLOUD_LLM_REQUEST_TIMEOUT", 300),
            "cloud_version": getattr(config, "CLOUD_LLM_VERSION", ""),
            "ollama_base_url": getattr(config, "OLLAMA_BASE_URL", ""),
            "ollama_model": getattr(config, "OLLAMA_CHAT_MODEL", ""),
        }
    except Exception:
        return {
            "enable_llm": False,
            "provider": "vllm",
            "body_limit": 12000,
            "max_tokens": 80,
            "max_workers": 3,
            "max_concurrency": 3,
            "batch_size": 3,
            "fail_open": False,
            "vllm_base_url": "",
            "vllm_model": "",
            "vllm_api_key": "EMPTY",
            "vllm_timeout": 300,
            "cloud_base_url": "",
            "cloud_model": "",
            "cloud_api_key": "",
            "cloud_timeout": 300,
            "cloud_version": "",
            "ollama_base_url": "",
            "ollama_model": "",
        }


def extract_json_from_text(text: str) -> dict[str, Any]:
    if not text:
        return {}

    text = text.replace("```json", "").replace("```", "").strip()
    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or start >= end:
        return {}

    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        logging.warning(f"시장 영향 이벤트 판정 JSON 파싱 실패: {text}")
        return {}


def parse_keep_from_text(text: str) -> dict[str, Any]:

    if not text:
        return {}

    cleaned = text.replace("```json", "").replace("```", "").strip()
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
    lowered = cleaned.lower()

    parsed = extract_json_from_text(cleaned)

    if parsed:
        return parsed

    keep_match = re.search(
        r'["\']?keep["\']?\s*[:=]\s*["\']?(true|false|yes|no|keep|drop)["\']?', lowered
    )

    if keep_match:
        value = keep_match.group(1)
        return {"keep": value in ["true", "yes", "keep"]}

    stripped = lowered.strip()

    if stripped in ["true", "yes", "keep", "kept"]:
        return {"keep": True}

    if stripped in ["false", "no", "drop", "dropped", "remove", "delete"]:
        return {"keep": False}

    leading_decision_match = re.match(
        r"^\s*(keep|drop|true|false|yes|no)\b",
        lowered,
        flags=re.IGNORECASE,
    )

    if leading_decision_match:
        decision = leading_decision_match.group(1).lower()
        return {"keep": decision in ["keep", "true", "yes"]}

    if re.search(r"\bdrop\b|\bremove\b|삭제|제거|버림|불필요", lowered):
        return {"keep": False}

    if re.search(r"유지|보존|남김|남겨", lowered):
        return {"keep": True}

    return {}


def parse_batch_keep_from_text(
    text: str,
    expected_count: int,
) -> dict[int, dict[str, Any]]:
    """
    batch 응답을 파싱한다.

    권장 응답:
    0: KEEP
    1: DROP
    """

    parsed_json = extract_json_from_text(text)

    if parsed_json and isinstance(parsed_json.get("results"), list):
        return normalize_batch_llm_material_event_results(
            parsed=parsed_json,
            expected_count=expected_count,
        )

    if not text:
        return {}

    cleaned = text.replace("```", "").strip()
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
    results = {}

    for line in cleaned.splitlines():
        line = line.strip()

        if not line:
            continue

        match = re.search(
            r"^(?:article\s*)?(\d+)\s*[:.)-]\s*(KEEP|DROP|TRUE|FALSE|YES|NO)\b",
            line,
            flags=re.IGNORECASE,
        )

        if not match:
            continue

        index = int(match.group(1))

        if index < 0 or index >= expected_count:
            continue

        decision = match.group(2).lower()
        normalized = normalize_llm_material_event_result(
            {"keep": decision in ["keep", "true", "yes"]},
        )
        normalized["provider"] = "llm_batch"
        results[index] = normalized

    return results


def build_material_event_source_text(item: dict[str, Any], body_limit: int = 12000) -> str:
    body_text = clean_article_body_for_storage(
        get_printable_text(item.get("_body_text", "")),
        article_title=get_printable_text(item.get("title", "")),
    )

    if body_limit and body_limit > 0:
        body_text = body_text[:body_limit]

    return body_text.strip()


def count_removed_noise_chars(entries: Any) -> int:

    if not isinstance(entries, list):
        return 0

    removed_chars = 0

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        before = get_printable_text(entry.get("text", ""))
        after = get_printable_text(entry.get("cleaned_text", ""))
        removed_chars += max(len(before) - len(after), 0)

    return removed_chars


def analyze_material_event_body_quality(
    item: dict[str, Any],
    body_limit: int = 12000,
) -> dict[str, Any]:

    raw_body = get_printable_text(item.get("_body_text", ""))
    removed_during_check = []
    cleaned_body = clean_article_body_for_storage(
        raw_body,
        removed_noise=removed_during_check,
        article_title=get_printable_text(item.get("title", "")),
    )
    full_cleaned_chars = len(re.sub(r"\s+", "", cleaned_body))

    if body_limit and body_limit > 0:
        cleaned_body = cleaned_body[:body_limit].strip()

    raw_chars = len(re.sub(r"\s+", "", raw_body))
    current_removed_chars = max(raw_chars - full_cleaned_chars, 0)
    recorded_removed_chars = count_removed_noise_chars(item.get("_body_noise_removed", []))
    removed_chars = current_removed_chars + recorded_removed_chars
    total_observed_chars = full_cleaned_chars + removed_chars
    noise_ratio = removed_chars / total_observed_chars if total_observed_chars > 0 else 1.0

    reason = ""

    if not cleaned_body:
        reason = "광고/저작권/매체 소개 등 노이즈만 존재"
    elif full_cleaned_chars < MIN_ARTICLE_BODY_CHARS:
        reason = "뉴스 사건을 판정하기에 지나치게 짧음"
    elif (
        noise_ratio >= NOISE_DOMINATED_RATIO and full_cleaned_chars < NOISE_DOMINATED_BODY_MAX_CHARS
    ):
        reason = "대부분이 광고/저작권/매체 소개 등 비기사 문구"

    return {
        "drop": bool(reason),
        "reason": reason,
        "cleaned_body": cleaned_body,
        "body_chars": full_cleaned_chars,
        "removed_noise_chars": removed_chars,
        "noise_ratio": round(noise_ratio, 4),
        "removed_noise_reasons": sorted(
            {
                reason_name
                for entry in removed_during_check
                if isinstance(entry, dict)
                for reason_name in entry.get("reasons", [])
                if isinstance(reason_name, str)
            }
        ),
    }


def build_deterministic_body_quality_result(
    item: dict[str, Any],
    body_limit: int = 12000,
) -> dict[str, Any] | None:
    quality = analyze_material_event_body_quality(
        item=item,
        body_limit=body_limit,
    )

    if not quality["drop"]:
        return None

    return {
        "keep": False,
        "provider": "deterministic_body_quality",
        "reason": quality["reason"],
        "body_quality": {
            key: value
            for key, value in quality.items()
            if key not in {"drop", "reason", "cleaned_body"}
        },
    }


def analyze_simple_market_wrap(item: dict[str, Any]) -> dict[str, Any]:

    body_text = build_material_event_source_text(item=item, body_limit=12000)
    has_index = bool(MARKET_INDEX_PATTERN.search(body_text))
    has_movement = bool(MARKET_MOVEMENT_PATTERN.search(body_text))
    observation_count = len(MARKET_OBSERVATION_PATTERN.findall(body_text))
    has_verified_event = bool(VERIFIED_NON_PRICE_EVENT_PATTERN.search(body_text))
    has_relation_event_signal = bool(MATERIAL_RELATION_SURFACE_PATTERN.search(body_text))
    is_simple_market_wrap = (
        has_index and has_movement and not has_verified_event and not has_relation_event_signal
    )

    return {
        "drop": is_simple_market_wrap,
        "reason": (
            "실제 비가격 사건 없이 주가지수 등락·수급만 전달한 단순 시황 기사"
            if is_simple_market_wrap
            else ""
        ),
        "has_market_index": has_index,
        "has_market_movement": has_movement,
        "market_observation_count": observation_count,
        "has_verified_non_price_event": has_verified_event,
        "has_relation_event_signal": has_relation_event_signal,
    }


def build_deterministic_market_wrap_result(
    item: dict[str, Any],
) -> dict[str, Any] | None:
    market_wrap = analyze_simple_market_wrap(item)

    if not market_wrap["drop"]:
        return None

    return {
        "keep": False,
        "provider": "deterministic_market_wrap",
        "reason": market_wrap["reason"],
        "market_wrap": {
            key: value for key, value in market_wrap.items() if key not in {"drop", "reason"}
        },
    }


def build_deterministic_material_event_result(
    item: dict[str, Any],
    body_limit: int = 12000,
) -> dict[str, Any] | None:

    return build_deterministic_body_quality_result(
        item, body_limit
    ) or build_deterministic_market_wrap_result(item)


def build_material_relation_schema_text() -> str:

    return "\n".join(
        f"{predicate}:{'/'.join(entity_types['subject'])}>{'/'.join(entity_types['object'])}"
        for predicate, entity_types in MATERIAL_RELATION_SCHEMA.items()
    )


def build_material_event_context(pipeline_input: dict[str, Any] | None) -> str:
    if not pipeline_input:
        return load_material_event_prompt_text("material_event_default_context.txt")

    theme_name = pipeline_input.get("theme_name", "")
    theme_description = pipeline_input.get("theme_description", "")
    companies = pipeline_input.get("companies", [])

    company_lines = []

    for company in companies:
        company_lines.append(
            f"- {company.get('company_name', '')}"
            f"({company.get('stock_code', '')}): "
            f"{company.get('inclusion_reason', '')}"
        )

    return render_material_event_prompt(
        "material_event_context.txt",
        theme_name=str(theme_name),
        theme_description=str(theme_description),
        company_lines="\n".join(company_lines),
    )


@lru_cache
def load_material_event_prompt_text(filename: str) -> str:

    return (MATERIAL_EVENT_PROMPT_DIRECTORY / filename).read_text(encoding="utf-8").strip()


def render_material_event_prompt(filename: str, **values: str) -> str:
    return Template(load_material_event_prompt_text(filename)).substitute(**values).strip()


def build_material_event_policy_text() -> str:

    return load_material_event_prompt_text("material_event_policy.txt")


def build_llm_material_event_prompt(
    item: dict[str, Any], pipeline_input: dict[str, Any] | None, body_limit: int = 12000
) -> str:
    source_text = build_material_event_source_text(item, body_limit)
    context_text = build_material_event_context(pipeline_input)
    policy_text = build_material_event_policy_text()
    relation_schema_text = build_material_relation_schema_text()
    title = get_printable_text(item.get("title", ""))
    description = get_printable_text(item.get("description", ""))

    return render_material_event_prompt(
        "material_event_single.txt",
        policy_text=policy_text,
        relation_schema_text=relation_schema_text,
        impact_channels="|".join(sorted(MATERIAL_IMPACT_CHANNELS)),
        context_text=context_text,
        title=title,
        description=description,
        source_text=source_text,
    )


def build_batch_llm_material_event_prompt(
    items: list[dict[str, Any]], pipeline_input: dict[str, Any] | None, body_limit: int = 12000
) -> str:
    context_text = build_material_event_context(pipeline_input)
    policy_text = build_material_event_policy_text()
    relation_schema_text = build_material_relation_schema_text()
    articles_text = []

    for index, item in enumerate(items):
        articles_text.append(
            render_material_event_prompt(
                "material_event_article.txt",
                index=str(index),
                title=get_printable_text(item.get("title", "")),
                description=get_printable_text(item.get("description", "")),
                source_text=build_material_event_source_text(item, body_limit),
            )
        )

    return render_material_event_prompt(
        "material_event_batch.txt",
        policy_text=policy_text,
        relation_schema_text=relation_schema_text,
        last_index=str(len(items) - 1),
        context_text=context_text,
        articles_text="\n".join(articles_text),
    )


def normalize_llm_boolean(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ["true", "yes", "1", "keep"]

    return bool(value)


def normalize_llm_material_event_result(
    parsed: dict[str, Any],
) -> dict[str, Any]:
    keep = parsed.get("keep")

    if keep is None:
        keep = parsed.get("is_market_moving_event", False)

    keep = normalize_llm_boolean(keep)
    gate_keys = ("has_relation", "is_material", "is_confirmed")

    if any(key in parsed for key in gate_keys):
        keep = keep and all(normalize_llm_boolean(parsed.get(key, False)) for key in gate_keys)

    return {
        "keep": keep,
        "provider": "llm",
    }


def normalize_batch_llm_material_event_results(
    parsed: dict[str, Any],
    expected_count: int,
) -> dict[int, dict[str, Any]]:
    results = parsed.get("results", [])

    if not isinstance(results, list):
        return {}

    normalized_results = {}

    for default_index, result_item in enumerate(results):
        if not isinstance(result_item, dict):
            continue

        try:
            index = int(result_item.get("index", default_index))
        except (TypeError, ValueError):
            continue

        if index < 0 or index >= expected_count:
            continue

        normalized = normalize_llm_material_event_result(
            result_item,
        )
        normalized["provider"] = "llm_batch"
        normalized_results[index] = normalized

    return normalized_results


def build_safe_keep_result(provider: str = "llm_error") -> dict[str, Any]:
    return {
        "keep": True,
        "provider": provider,
    }


def build_llm_failure_result(
    provider: str,
    fail_open: bool = False,
) -> dict[str, Any]:

    return {
        "keep": bool(fail_open),
        "provider": provider,
        "reason": (
            "LLM 판정 실패로 보수적 유지"
            if fail_open
            else "LLM 판정 실패 또는 결과 누락으로 엄격 필터에서 제거"
        ),
    }


def build_vllm_chat_completion_request(
    item: dict[str, Any], pipeline_input: dict[str, Any] | None, filter_config: dict[str, Any]
) -> tuple[str, dict[str, str], dict[str, Any], int]:
    base_url = str(filter_config.get("vllm_base_url") or "").rstrip("/")
    model = str(filter_config.get("vllm_model") or "")

    if not base_url or not model:
        raise RuntimeError("vLLM 이벤트 필터 설정이 비어있음")

    return (
        f"{base_url}/chat/completions",
        {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {filter_config.get('vllm_api_key') or 'EMPTY'}",
        },
        {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": load_material_event_prompt_text("material_event_single_system.txt"),
                },
                {
                    "role": "user",
                    "content": build_llm_material_event_prompt(
                        item=item,
                        pipeline_input=pipeline_input,
                        body_limit=int(filter_config.get("body_limit") or 12000),
                    ),
                },
            ],
            "temperature": 0.0,
            "max_tokens": int(filter_config.get("max_tokens") or 80),
            "chat_template_kwargs": {
                "enable_thinking": False,
            },
            "stream": False,
        },
        int(filter_config.get("vllm_timeout") or 300),
    )


def parse_vllm_material_event_response(
    response_data: dict[str, Any],
) -> dict[str, Any]:
    choices = response_data.get("choices", [])

    if not choices:
        raise RuntimeError("vLLM 이벤트 필터 choices가 비어있음")

    raw_response = choices[0].get("message", {}).get("content", "")
    parsed = parse_keep_from_text(raw_response)

    if not parsed:
        logging.warning(f"vLLM 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("vLLM 이벤트 필터 JSON 파싱 실패")

    return normalize_llm_material_event_result(parsed)


def judge_material_event_with_vllm(
    item: dict[str, Any], pipeline_input: dict[str, Any] | None, filter_config: dict[str, Any]
) -> dict[str, Any]:
    import requests

    url, headers, payload, timeout = build_vllm_chat_completion_request(
        item=item,
        pipeline_input=pipeline_input,
        filter_config=filter_config,
    )
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()

    return parse_vllm_material_event_response(response.json())


async def judge_material_event_with_vllm_async(
    item: dict[str, Any],
    pipeline_input: dict[str, Any] | None,
    filter_config: dict[str, Any],
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    url, headers, payload, timeout = build_vllm_chat_completion_request(
        item=item,
        pipeline_input=pipeline_input,
        filter_config=filter_config,
    )
    response = await client.post(
        url,
        headers=headers,
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()

    return parse_vllm_material_event_response(response.json())


def extract_anthropic_text_content(response_data: dict[str, Any]) -> str:

    content = response_data.get("content", [])

    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            return block.get("text", "")

    return ""


def judge_material_event_with_cloud(
    item: dict[str, Any], pipeline_input: dict[str, Any] | None, filter_config: dict[str, Any]
) -> dict[str, Any]:

    import requests

    base_url = str(filter_config.get("cloud_base_url", "")).rstrip("/")
    model = str(filter_config.get("cloud_model", ""))
    api_key = str(filter_config.get("cloud_api_key", ""))
    anthropic_version = str(filter_config.get("cloud_version"))

    if not base_url or not model or not api_key:
        raise RuntimeError("클라우드 LLM 이벤트 필터 설정이 비어있음")

    response = requests.post(
        f"{base_url}/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": anthropic_version,
        },
        json={
            "model": model,
            "system": load_material_event_prompt_text("material_event_single_system.txt"),
            "messages": [
                {
                    "role": "user",
                    "content": build_llm_material_event_prompt(
                        item=item,
                        pipeline_input=pipeline_input,
                        body_limit=int(filter_config.get("body_limit") or 12000),
                    ),
                },
            ],
            "max_tokens": int(filter_config.get("max_tokens") or 80),
        },
        timeout=int(filter_config.get("cloud_timeout") or 300),
    )
    response.raise_for_status()

    raw_response = extract_anthropic_text_content(response.json())

    if not raw_response:
        raise RuntimeError("클라우드 LLM 이벤트 필터 content가 비어있음")

    parsed = parse_keep_from_text(raw_response)

    if not parsed:
        logging.warning(f"클라우드 LLM 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("클라우드 LLM 이벤트 필터 JSON 파싱 실패")

    return normalize_llm_material_event_result(parsed)


def judge_material_event_with_ollama(
    item: dict[str, Any], pipeline_input: dict[str, Any] | None, filter_config: dict[str, Any]
) -> dict[str, Any]:
    import requests

    base_url = str(filter_config.get("ollama_base_url", "")).rstrip("/")
    model = filter_config.get("ollama_model", "")

    if not base_url or not model:
        raise RuntimeError("Ollama 이벤트 필터 설정이 비어있음")

    response = requests.post(
        f"{base_url}/api/generate",
        json={
            "model": model,
            "prompt": build_llm_material_event_prompt(
                item=item,
                pipeline_input=pipeline_input,
                body_limit=int(filter_config.get("body_limit") or 12000),
            ),
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": int(filter_config.get("max_tokens") or 80),
            },
        },
        timeout=300,
    )
    response.raise_for_status()

    raw_response = response.json().get("response", "")
    parsed = parse_keep_from_text(raw_response)

    if not parsed:
        logging.warning(f"Ollama 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("Ollama 이벤트 필터 JSON 파싱 실패")

    return normalize_llm_material_event_result(parsed)


def judge_material_event_with_llm(
    item: dict[str, Any],
    pipeline_input: dict[str, Any] | None = None,
    filter_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    filter_config = filter_config or get_material_event_filter_config()
    provider = str(filter_config.get("provider") or "vllm").lower()

    if provider in {"cloud", "openai", "openai_compatible"}:
        return judge_material_event_with_cloud(
            item=item,
            pipeline_input=pipeline_input,
            filter_config=filter_config,
        )

    if provider == "ollama":
        return judge_material_event_with_ollama(
            item=item,
            pipeline_input=pipeline_input,
            filter_config=filter_config,
        )

    return judge_material_event_with_vllm(
        item=item,
        pipeline_input=pipeline_input,
        filter_config=filter_config,
    )


def judge_material_events_batch_with_vllm(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    filter_config: dict[str, Any],
) -> dict[int, dict[str, Any]]:
    import requests

    base_url = str(filter_config.get("vllm_base_url", "")).rstrip("/")
    model = filter_config.get("vllm_model", "")

    if not base_url or not model:
        raise RuntimeError("vLLM batch 이벤트 필터 설정이 비어있음")

    response = requests.post(
        f"{base_url}/chat/completions",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {filter_config.get('vllm_api_key') or 'EMPTY'}",
        },
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": load_material_event_prompt_text("material_event_batch_system.txt"),
                },
                {
                    "role": "user",
                    "content": build_batch_llm_material_event_prompt(
                        items=items,
                        pipeline_input=pipeline_input,
                        body_limit=int(filter_config.get("body_limit") or 12000),
                    ),
                },
            ],
            "temperature": 0.0,
            "max_tokens": int(filter_config.get("max_tokens") or 80) * max(len(items), 1),
            "chat_template_kwargs": {
                "enable_thinking": False,
            },
            "stream": False,
        },
        timeout=int(filter_config.get("vllm_timeout") or 300),
    )
    response.raise_for_status()

    choices = response.json().get("choices", [])

    if not choices:
        raise RuntimeError("vLLM batch 이벤트 필터 choices가 비어있음")

    raw_response = choices[0].get("message", {}).get("content", "")
    parsed = parse_batch_keep_from_text(
        text=raw_response,
        expected_count=len(items),
    )

    if not parsed:
        logging.warning(f"vLLM batch 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("vLLM batch 이벤트 필터 JSON 파싱 실패")

    return parsed


def judge_material_events_batch_with_cloud(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    filter_config: dict[str, Any],
) -> dict[int, dict[str, Any]]:

    import requests

    base_url = str(filter_config.get("cloud_base_url", "")).rstrip("/")
    model = str(filter_config.get("cloud_model", ""))
    api_key = str(filter_config.get("cloud_api_key", ""))
    anthropic_version = str(filter_config.get("cloud_version") or "2023-06-01")

    if not base_url or not model or not api_key:
        raise RuntimeError("클라우드 LLM batch 이벤트 필터 설정이 비어있음")

    response = requests.post(
        f"{base_url}/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": anthropic_version,
        },
        json={
            "model": model,
            "system": load_material_event_prompt_text("material_event_batch_system.txt"),
            "messages": [
                {
                    "role": "user",
                    "content": build_batch_llm_material_event_prompt(
                        items=items,
                        pipeline_input=pipeline_input,
                        body_limit=int(filter_config.get("body_limit") or 12000),
                    ),
                },
            ],
            "max_tokens": (int(filter_config.get("max_tokens") or 80) * max(len(items), 1)),
        },
        timeout=int(filter_config.get("cloud_timeout") or 300),
    )
    response.raise_for_status()

    raw_response = extract_anthropic_text_content(response.json())

    if not raw_response:
        raise RuntimeError("클라우드 LLM batch 이벤트 필터 content가 비어있음")

    parsed = parse_batch_keep_from_text(
        text=raw_response,
        expected_count=len(items),
    )

    if not parsed:
        logging.warning(f"클라우드 LLM batch 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("클라우드 LLM batch 이벤트 필터 JSON 파싱 실패")

    return parsed


def judge_material_events_batch_with_ollama(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    filter_config: dict[str, Any],
) -> dict[int, dict[str, Any]]:
    import requests

    base_url = str(filter_config.get("ollama_base_url", "")).rstrip("/")
    model = filter_config.get("ollama_model", "")

    if not base_url or not model:
        raise RuntimeError("Ollama batch 이벤트 필터 설정이 비어있음")

    response = requests.post(
        f"{base_url}/api/generate",
        json={
            "model": model,
            "prompt": build_batch_llm_material_event_prompt(
                items=items,
                pipeline_input=pipeline_input,
                body_limit=int(filter_config.get("body_limit") or 12000),
            ),
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": int(filter_config.get("max_tokens") or 80) * max(len(items), 1),
            },
        },
        timeout=300,
    )
    response.raise_for_status()

    raw_response = response.json().get("response", "")
    parsed = parse_batch_keep_from_text(
        text=raw_response,
        expected_count=len(items),
    )

    if not parsed:
        logging.warning(f"Ollama batch 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("Ollama batch 이벤트 필터 JSON 파싱 실패")

    return parsed


def judge_material_events_batch_with_llm(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    filter_config: dict[str, Any] | None = None,
) -> dict[int, dict[str, Any]]:
    filter_config = filter_config or get_material_event_filter_config()
    provider = str(filter_config.get("provider") or "vllm").lower()

    if provider in {"cloud", "openai", "openai_compatible"}:
        return judge_material_events_batch_with_cloud(
            items=items,
            pipeline_input=pipeline_input,
            filter_config=filter_config,
        )

    if provider == "ollama":
        return judge_material_events_batch_with_ollama(
            items=items,
            pipeline_input=pipeline_input,
            filter_config=filter_config,
        )

    return judge_material_events_batch_with_vllm(
        items=items,
        pipeline_input=pipeline_input,
        filter_config=filter_config,
    )


def build_material_event_analysis_for_item(
    item: dict[str, Any],
    pipeline_input: dict[str, Any] | None,
    use_llm: bool,
    filter_config: dict[str, Any] | None,
) -> dict[str, Any]:
    body_limit = int((filter_config or {}).get("body_limit") or 12000)
    deterministic_result = build_deterministic_material_event_result(
        item=item,
        body_limit=body_limit,
    )

    if deterministic_result:
        return deterministic_result

    if not use_llm:
        return build_safe_keep_result(provider="llm_disabled")

    try:
        return judge_material_event_with_llm(
            item=item,
            pipeline_input=pipeline_input,
            filter_config=filter_config or {},
        )
    except Exception as e:
        title = get_printable_text(item.get("title", ""))
        fail_open = bool((filter_config or {}).get("fail_open", False))
        logging.warning(
            f"LLM 이벤트 필터 실패: fail_open={fail_open}, "
            f"title={title}, error={type(e).__name__}: {e}"
        )
        return build_llm_failure_result(
            provider="llm_error",
            fail_open=fail_open,
        )


def build_material_event_analyses_sequential(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    use_llm: bool,
    filter_config: dict[str, Any] | None,
) -> dict[int, dict[str, Any]]:
    return {
        index: build_material_event_analysis_for_item(
            item=item,
            pipeline_input=pipeline_input,
            use_llm=use_llm,
            filter_config=filter_config,
        )
        for index, item in enumerate(items)
    }


async def build_material_event_analysis_for_item_async(
    item: dict[str, Any],
    pipeline_input: dict[str, Any] | None,
    use_llm: bool,
    filter_config: dict[str, Any] | None,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    body_limit = int((filter_config or {}).get("body_limit") or 12000)
    deterministic_result = build_deterministic_material_event_result(
        item=item,
        body_limit=body_limit,
    )

    if deterministic_result:
        return deterministic_result

    if not use_llm:
        return build_safe_keep_result(provider="llm_disabled")

    try:
        async with semaphore:
            return await judge_material_event_with_vllm_async(
                item=item,
                pipeline_input=pipeline_input,
                filter_config=filter_config or {},
                client=client,
            )
    except Exception as e:
        title = get_printable_text(item.get("title", ""))
        fail_open = bool((filter_config or {}).get("fail_open", False))
        logging.warning(
            f"vLLM 비동기 이벤트 필터 실패: fail_open={fail_open}, "
            f"title={title}, error={type(e).__name__}: {e}"
        )
        return build_llm_failure_result(
            provider="llm_error",
            fail_open=fail_open,
        )


async def build_material_event_analyses_async(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    use_llm: bool,
    filter_config: dict[str, Any] | None,
    max_concurrency: int,
) -> dict[int, dict[str, Any]]:
    max_concurrency = max(int(max_concurrency or 1), 1)
    semaphore = asyncio.Semaphore(max_concurrency)
    limits = httpx.Limits(
        max_connections=max_concurrency,
        max_keepalive_connections=max_concurrency,
    )

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            build_material_event_analysis_for_item_async(
                item=item,
                pipeline_input=pipeline_input,
                use_llm=use_llm,
                filter_config=filter_config,
                client=client,
                semaphore=semaphore,
            )
            for item in items
        ]
        results = await asyncio.gather(*tasks)

    return {index: analysis for index, analysis in enumerate(results)}


def build_material_event_analyses_threaded(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    use_llm: bool,
    filter_config: dict[str, Any] | None,
    max_workers: int,
) -> dict[int, dict[str, Any]]:
    analyses = {}
    max_workers = max(int(max_workers or 1), 1)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                build_material_event_analysis_for_item,
                item,
                pipeline_input,
                use_llm,
                filter_config,
            ): index
            for index, item in enumerate(items)
        }

        for future in as_completed(futures):
            index = futures[future]
            analyses[index] = future.result()

    return analyses


def build_material_event_analyses_batch(
    items: list[dict[str, Any]],
    pipeline_input: dict[str, Any] | None,
    use_llm: bool,
    filter_config: dict[str, Any] | None,
    batch_size: int,
) -> dict[int, dict[str, Any]]:
    if not use_llm:
        return build_material_event_analyses_sequential(
            items=items,
            pipeline_input=pipeline_input,
            use_llm=False,
            filter_config=filter_config,
        )

    analyses = {}
    batch_size = max(int(batch_size or 1), 1)

    for batch_start in range(0, len(items), batch_size):
        batch_items = items[batch_start : batch_start + batch_size]
        llm_items = []
        llm_local_indexes = []

        for local_index, item in enumerate(batch_items):
            deterministic_result = build_deterministic_material_event_result(
                item=item,
                body_limit=int((filter_config or {}).get("body_limit") or 12000),
            )

            if deterministic_result:
                analyses[batch_start + local_index] = deterministic_result
                continue

            llm_items.append(item)
            llm_local_indexes.append(local_index)

        if not llm_items:
            continue

        try:
            batch_results = judge_material_events_batch_with_llm(
                items=llm_items,
                pipeline_input=pipeline_input,
                filter_config=filter_config,
            )
        except Exception as e:
            logging.warning(
                f"LLM batch 이벤트 필터 실패로 단건 재시도: "
                f"batch_start={batch_start}, error={type(e).__name__}: {e}"
            )
            batch_results = {}

        for llm_index, (local_index, item) in enumerate(
            zip(
                llm_local_indexes,
                llm_items,
                strict=True,
            )
        ):
            global_index = batch_start + local_index
            analysis = batch_results.get(llm_index)

            if not analysis:
                analysis = build_material_event_analysis_for_item(
                    item=item,
                    pipeline_input=pipeline_input,
                    use_llm=True,
                    filter_config=filter_config,
                )

                if analysis.get("provider") == "llm":
                    analysis["provider"] = "llm_single_retry"

            analyses[global_index] = analysis

    return analyses


def split_items_by_material_event_result(
    items: list[dict[str, Any]], analyses: dict[int, dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    passed_items = []
    removed_items = []

    for index, item in enumerate(items):
        analysis = analyses.get(index) or build_llm_failure_result(
            provider="missing_result",
            fail_open=False,
        )
        item["_material_event"] = analysis

        if analysis.get("keep"):
            passed_items.append(item)
        else:
            removed_items.append(
                {
                    "removed_item": item,
                    "reason": analysis.get("reason") or "시장/기업 영향 기사 아님",
                    "debug_info": {
                        "material_event": analysis,
                    },
                }
            )

    return passed_items, removed_items


def filter_material_event_news(
    items: list[dict[str, Any]],
    min_score: int = 0,
    pipeline_input: dict[str, Any] | None = None,
    use_llm: bool | None = None,
    min_confidence: float | None = None,
    mode: str = "sequential",
    max_workers: int | None = None,
    max_concurrency: int | None = None,
    batch_size: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    filter_config = get_material_event_filter_config()

    if use_llm is None:
        use_llm = bool(filter_config.get("enable_llm"))

    if max_workers is None:
        max_workers = int(filter_config.get("max_workers", 3))

    if max_concurrency is None:
        max_concurrency = int(filter_config.get("max_concurrency", 3))

    if batch_size is None:
        batch_size = int(filter_config.get("batch_size", 3))

    mode = (mode or "sequential").lower()
    provider = str(filter_config.get("provider") or "vllm").lower()
    is_vllm_provider = provider not in {
        "cloud",
        "openai",
        "openai_compatible",
        "ollama",
    }

    if mode == "async":
        if not is_vllm_provider:
            raise ValueError("async mode는 vLLM provider에서만 지원합니다.")

        analyses = asyncio.run(
            build_material_event_analyses_async(
                items=items,
                pipeline_input=pipeline_input,
                use_llm=bool(use_llm),
                filter_config=filter_config,
                max_concurrency=max_concurrency,
            )
        )
    elif mode == "thread":
        analyses = build_material_event_analyses_threaded(
            items=items,
            pipeline_input=pipeline_input,
            use_llm=bool(use_llm),
            filter_config=filter_config,
            max_workers=max_workers,
        )
    elif mode == "batch":
        analyses = build_material_event_analyses_batch(
            items=items,
            pipeline_input=pipeline_input,
            use_llm=bool(use_llm),
            filter_config=filter_config,
            batch_size=batch_size,
        )
    else:
        analyses = build_material_event_analyses_sequential(
            items=items,
            pipeline_input=pipeline_input,
            use_llm=bool(use_llm),
            filter_config=filter_config,
        )

    return split_items_by_material_event_result(
        items=items,
        analyses=analyses,
    )
