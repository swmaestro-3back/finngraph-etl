"""투표 판정 프롬프트가 글자 하나도 바뀌지 않았는지 해시로 확인한다. 프롬프트를 일부러 바꿨다면 해시와
함께 llm.PROMPT_VERSIONS 도 올려야 캐시에 남은 옛 응답을 다시 쓰지 않는다.

문자열은 sha256(텍스트), 도구 정의와 목록은 sha256(json.dumps(값, ensure_ascii=False, sort_keys=True))
로 계산한다. 판정 예시는 문장 목록만 비교하고, 예시마다 붙인 주요 기업 이름은 해시에 넣지 않는다.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from pipelines.news.transformers.prompts import issue_kind as K
from pipelines.news.transformers.prompts import issue_link_confirm as C
from pipelines.news.transformers.prompts import issue_link_pair as P
from pipelines.news.transformers.prompts import issue_link_rank as R

EXPECTED = {
    "SCREEN_PROMPT": "799bdfdad203cdab6605a6aab62531647ec42d46c42ebcdab48598def2cda6a4",
    "EVIDENCE_TOOL_PROMPT": "c4a4ef49fcfc441ba64a5c7ff8894804eb2d56cebd029fb58c8986d61aecb47f",
    "EVIDENCE_TOOL": "6b5394d27a4ba709b4b8a7e66da317217e85b8b95ecfb215d8105d0aeb8a502d",
    "MATTER_PROMPT": "a0355981471f2901082f6b59efcbbf1c19946bca8b660987460e83f839c84ed9",
    "MATTER_TOOL": "f2dba53981efd9a686f3de0cbe6c1da92c36bfbf0ac6365ce2918754f73e1c25",
    "MATTER_ACCEPTED": "477665fea5b51479c4bb1b82a939fad1ebd87a49dbb44a419a7ded0d74199f9e",
    "PLAN_EXTRACT_PROMPT": "564758bb828b7b5af7405f1ea41f1c8795ded8426db9c0eb0f8793856f62b29d",
    "PLAN_EXTRACT_TOOL": "69fdf621f8c5ca26081f9d93e9895cd74316ba81b6fd14c03db0273e972507c0",
    "PLANSTEP_PROMPT": "ff823888f036f36cfa017e01a313a2cb490f70e6b578c6bedf0e16ab5a47bddc",
    "PLANSTEP_TOOL": "c6e4436e90f7c9233ee0a4b21e963a2bdc0482149d545b23bdf13517cee417d9",
    "EVIDENCE3_TOOL_PROMPT": "9c59ff346be3be7cbc555f798225bb80edcec04943c8cb0a9afc6717cd927c78",
    "EVIDENCE3_TOOL": "8bc6389c050b97976ced0ae80300c6171e7c26923d80805a2ca60c6061c4b2a3",
    "RANK_SYSTEM_V8": "cbaad1facf9cd61b2c72d54d3cafdbca3171875a4c9ea3893a299f63e0dc375c",
    "JUDGE_SYSTEM": "70d43e5c49dab927f3d6afecf739f88994f87df7a6bb293fba4a4f5f179a1952",
    "JUDGE_PROMPT": "c0cf9c48d48322801900de9d46fc7ab8661946552fa539a9be407ef1f8bab36b",
    "JUDGE_EXAMPLES": "66a33b41f1478b26500e576f81a18a95f870fa9633be9c7cd82b07ae2fa8b104",
    "CHECK_PROMPT": "30c654f2629a32fe1e90b6a799fcb5f4780f81c9da6336e9f853ebced650ad0e",
    "PLAN_READER_PROMPT": "fbebd56a5012863010f89c344d1d00da10b9d1b0adbec1cef28853c375ae454a",
    "PLAN_READER_EXAMPLES": "f771c0773717406c480a8418d50c5ea0371a00abb8af46e94b16e96f5e950372",
    "KIND_SYSTEM": "2eb71deff5f309d8f22338456a0add4905fce8a641316472009f5b09ee3a474d",
    "KIND_TOOL": "3f6fefa2b567e649e7e4e90e268ee51c41ff02ab1d6e274ebce8dc0c7876f604",
}

ACTUAL = {
    "SCREEN_PROMPT": P.SCREEN_PROMPT,
    "EVIDENCE_TOOL_PROMPT": P.EVIDENCE_TOOL_PROMPT,
    "EVIDENCE_TOOL": P.EVIDENCE_TOOL,
    "MATTER_PROMPT": P.MATTER_PROMPT,
    "MATTER_TOOL": P.MATTER_TOOL,
    "MATTER_ACCEPTED": sorted(P.MATTER_ACCEPTED),
    "PLAN_EXTRACT_PROMPT": P.PLAN_EXTRACT_PROMPT,
    "PLAN_EXTRACT_TOOL": P.PLAN_EXTRACT_TOOL,
    "PLANSTEP_PROMPT": P.PLANSTEP_PROMPT,
    "PLANSTEP_TOOL": P.PLANSTEP_TOOL,
    "EVIDENCE3_TOOL_PROMPT": P.EVIDENCE3_TOOL_PROMPT,
    "EVIDENCE3_TOOL": P.EVIDENCE3_TOOL,
    "RANK_SYSTEM_V8": R.SYSTEM_V8,
    "JUDGE_SYSTEM": C.SYSTEM,
    "JUDGE_PROMPT": C.JUDGE_PROMPT,
    "JUDGE_EXAMPLES": [text for _, text in C.JUDGE_EXAMPLES],
    "CHECK_PROMPT": C.CHECK_PROMPT,
    "PLAN_READER_PROMPT": C.PLAN_READER_PROMPT,
    "PLAN_READER_EXAMPLES": [text for _, text in C.PLAN_EXAMPLES],
    "KIND_SYSTEM": K.SYSTEM,
    "KIND_TOOL": K.TOOL,
}


def _sha(value) -> str:
    text = (
        value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
    )
    return hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_prompt_is_verbatim(name):
    assert _sha(ACTUAL[name]) == EXPECTED[name]


def test_examples_carry_company_names_not_experiment_ids():
    for companies, text in C.JUDGE_EXAMPLES + C.PLAN_EXAMPLES:
        assert isinstance(companies, tuple)
        assert all(isinstance(name, str) and name for name in companies)
        assert text
    # 기업이 없는 일반 예시는 scope 판정의 두 개뿐이다.
    assert [text for companies, text in C.JUDGE_EXAMPLES if not companies] == [
        text for _, text in C.JUDGE_EXAMPLES[-2:]
    ]
