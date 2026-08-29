import asyncio
import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any

from pipelines.common.clients.bedrock import extract_bedrock_text, get_bedrock_client
from pipelines.common.config import get_settings
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)
from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT

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
        from pipelines.news.config import get_news_settings

        settings = get_settings()
        news_settings = get_news_settings()

        return {
            "enable_llm": True,
            "body_limit": news_settings.news_llm_body_limit,
            "max_tokens": news_settings.news_llm_max_tokens,
            "max_concurrency": news_settings.news_llm_max_concurrency,
            "fail_open": news_settings.material_event_filter_fail_open,
            "bedrock_region": settings.bedrock_region,
            "bedrock_model": settings.bedrock_chat_model,
            "bedrock_timeout": settings.bedrock_request_timeout,
        }
    except Exception:
        return {
            "enable_llm": False,
            "body_limit": 12000,
            "max_tokens": 512,
            "max_concurrency": 3,
            "fail_open": False,
            "bedrock_region": "",
            "bedrock_model": "",
            "bedrock_timeout": 300,
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


def build_material_event_source_text(item: dict[str, Any], body_limit: int = 12000) -> str:
    text = clean_article_body_for_storage(
        get_printable_text(item.get("_text", "")),
        article_title=get_printable_text(item.get("title", "")),
    )

    if body_limit and body_limit > 0:
        text = text[:body_limit]

    return text.strip()


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

    raw_body = get_printable_text(item.get("_text", ""))
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

    text = build_material_event_source_text(item=item, body_limit=12000)
    has_index = bool(MARKET_INDEX_PATTERN.search(text))
    has_movement = bool(MARKET_MOVEMENT_PATTERN.search(text))
    observation_count = len(MARKET_OBSERVATION_PATTERN.findall(text))
    has_verified_event = bool(VERIFIED_NON_PRICE_EVENT_PATTERN.search(text))
    has_relation_event_signal = bool(MATERIAL_RELATION_SURFACE_PATTERN.search(text))
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


def build_material_predicate_schema_text() -> str:
    """PREDICATE_DICT 온톨로지를 필터 프롬프트용 항목 목록으로 렌더링한다."""

    lines = []

    for name in sorted(PREDICATE_DICT):
        spec = PREDICATE_DICT[name]
        arguments = list(spec["arguments"].values())
        subject_types = "/".join(arguments[0]["types"])
        object_types = "/".join(arguments[1]["types"])
        line = f"- {name} ({subject_types} > {object_types}): {spec['description']}"

        if len(arguments) > 2:
            requirement = "mandatory" if arguments[2].get("required") else "optional"
            line += f" (item: {'/'.join(arguments[2]['types'])} [{requirement}])"

        lines.append(line)

    return "\n".join(lines)


@lru_cache
def load_material_event_prompt_text(filename: str) -> str:

    return (MATERIAL_EVENT_PROMPT_DIRECTORY / filename).read_text(encoding="utf-8").strip()


def render_material_event_prompt(filename: str, **values: str) -> str:
    return Template(load_material_event_prompt_text(filename)).substitute(**values).strip()


def build_material_event_policy_text() -> str:

    return load_material_event_prompt_text("material_event_policy.txt")


def build_llm_material_event_prompt(item: dict[str, Any], body_limit: int = 12000) -> str:
    return render_material_event_prompt(
        "material_event_single.txt",
        policy_text=build_material_event_policy_text(),
        relation_schema_text=build_material_predicate_schema_text(),
        title=get_printable_text(item.get("title", "")),
        description=get_printable_text(item.get("description", "")),
        source_text=build_material_event_source_text(item, body_limit),
    )


def normalize_llm_boolean(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ["true", "yes", "1", "keep"]

    return bool(value)


def normalize_llm_material_event_result(
    parsed: dict[str, Any],
) -> dict[str, Any]:
    keep = normalize_llm_boolean(parsed.get("keep"))
    gate_keys = ("has_relation", "is_material", "is_confirmed")
    # 게이트·관계 값을 결과에 보존해 drop/keep 근거를 사후 추적할 수 있게 한다.
    gates = {key: normalize_llm_boolean(parsed[key]) for key in gate_keys if key in parsed}

    if gates:
        keep = keep and all(gates.get(key, False) for key in gate_keys)

    relation = parsed.get("relation")

    return {
        "keep": keep,
        "provider": "llm",
        "gates": gates,
        "relation": relation if isinstance(relation, dict) else None,
    }


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


def build_bedrock_converse_request(
    item: dict[str, Any], filter_config: dict[str, Any]
) -> tuple[str, str, str, dict[str, Any]]:
    model_id = str(filter_config.get("bedrock_model") or "")

    if not model_id:
        raise RuntimeError("Bedrock 이벤트 필터 설정이 비어있음(모델 ID 필요)")

    return (
        model_id,
        load_material_event_prompt_text("material_event_single_system.txt"),
        build_llm_material_event_prompt(
            item=item,
            body_limit=int(filter_config.get("body_limit") or 12000),
        ),
        {
            "temperature": 0.0,
            # relation 객체가 keep보다 먼저 출력되므로, 폴백이 너무 작으면 절단 시
            # JSON 파싱 실패 → 일괄 drop으로 이어진다. 넉넉히 둔다.
            "maxTokens": int(filter_config.get("max_tokens") or 256),
        },
    )


def parse_bedrock_material_event_response(
    response_data: dict[str, Any],
) -> dict[str, Any]:
    raw_response = extract_bedrock_text(response_data)
    parsed = parse_keep_from_text(raw_response)

    if not parsed:
        logging.warning(f"Bedrock 이벤트 필터 응답 파싱 실패: {raw_response[:500]}")
        raise RuntimeError("Bedrock 이벤트 필터 JSON 파싱 실패")

    return normalize_llm_material_event_result(parsed)


def judge_material_event_with_bedrock(
    item: dict[str, Any], filter_config: dict[str, Any]
) -> dict[str, Any]:
    model_id, system_text, user_text, inference_config = build_bedrock_converse_request(
        item=item,
        filter_config=filter_config,
    )
    client = get_bedrock_client(
        str(filter_config.get("bedrock_region") or ""),
        int(filter_config.get("bedrock_timeout") or 300),
    )
    response = client.converse(
        modelId=model_id,
        system=[{"text": system_text}],
        messages=[{"role": "user", "content": [{"text": user_text}]}],
        inferenceConfig=inference_config,
    )

    return parse_bedrock_material_event_response(response)


async def judge_material_event_with_bedrock_async(
    item: dict[str, Any],
    filter_config: dict[str, Any],
) -> dict[str, Any]:
    # boto3는 동기 클라이언트라 to_thread로 감싸 asyncio.Semaphore 동시성만 활용한다.
    return await asyncio.to_thread(
        judge_material_event_with_bedrock,
        item,
        filter_config,
    )


def judge_material_event_with_llm(
    item: dict[str, Any],
    filter_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    filter_config = filter_config or get_material_event_filter_config()

    return judge_material_event_with_bedrock(
        item=item,
        filter_config=filter_config,
    )


def log_llm_material_event_drop(item: dict[str, Any], result: dict[str, Any]) -> None:
    if result.get("keep"):
        return

    logging.info(
        "LLM 이벤트 필터 drop: title=%s, gates=%s, relation=%s",
        get_printable_text(item.get("title", "")),
        result.get("gates"),
        result.get("relation"),
    )


def build_material_event_analysis_for_item(
    item: dict[str, Any],
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
        result = judge_material_event_with_llm(
            item=item,
            filter_config=filter_config or {},
        )
        log_llm_material_event_drop(item, result)
        return result
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
    use_llm: bool,
    filter_config: dict[str, Any] | None,
) -> dict[int, dict[str, Any]]:
    return {
        index: build_material_event_analysis_for_item(
            item=item,
            use_llm=use_llm,
            filter_config=filter_config,
        )
        for index, item in enumerate(items)
    }


async def build_material_event_analysis_for_item_async(
    item: dict[str, Any],
    use_llm: bool,
    filter_config: dict[str, Any] | None,
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
            result = await judge_material_event_with_bedrock_async(
                item=item,
                filter_config=filter_config or {},
            )
        log_llm_material_event_drop(item, result)
        return result
    except Exception as e:
        title = get_printable_text(item.get("title", ""))
        fail_open = bool((filter_config or {}).get("fail_open", False))
        logging.warning(
            f"Bedrock 비동기 이벤트 필터 실패: fail_open={fail_open}, "
            f"title={title}, error={type(e).__name__}: {e}"
        )
        return build_llm_failure_result(
            provider="llm_error",
            fail_open=fail_open,
        )


async def build_material_event_analyses_async(
    items: list[dict[str, Any]],
    use_llm: bool,
    filter_config: dict[str, Any] | None,
    max_concurrency: int,
) -> dict[int, dict[str, Any]]:
    semaphore = asyncio.Semaphore(max(int(max_concurrency or 1), 1))
    tasks = [
        build_material_event_analysis_for_item_async(
            item=item,
            use_llm=use_llm,
            filter_config=filter_config,
            semaphore=semaphore,
        )
        for item in items
    ]
    results = await asyncio.gather(*tasks)

    return {index: analysis for index, analysis in enumerate(results)}


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
    use_llm: bool | None = None,
    mode: str = "sequential",
    max_concurrency: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    filter_config = get_material_event_filter_config()

    if use_llm is None:
        use_llm = bool(filter_config.get("enable_llm"))

    if max_concurrency is None:
        max_concurrency = int(filter_config.get("max_concurrency", 3))

    mode = (mode or "sequential").lower()

    if mode == "async":
        analyses = asyncio.run(
            build_material_event_analyses_async(
                items=items,
                use_llm=bool(use_llm),
                filter_config=filter_config,
                max_concurrency=max_concurrency,
            )
        )
    else:
        analyses = build_material_event_analyses_sequential(
            items=items,
            use_llm=bool(use_llm),
            filter_config=filter_config,
        )

    return split_items_by_material_event_result(
        items=items,
        analyses=analyses,
    )
