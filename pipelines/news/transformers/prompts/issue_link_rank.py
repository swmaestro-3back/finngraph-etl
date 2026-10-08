"""이슈 연결 rank 투표자 프롬프트다(프롬프트 버전 rank-v8). tests/news/test_issue_link_vote_prompts.py 가
해시로 고정하므로, 바꿀 때는 해시와 llm.PROMPT_VERSIONS 를 함께 올린다.

SYSTEM_V8 은 이전 버전의 문자열을 조합하고 치환해 만든다(v5 라벨 규칙 → v6 필드 → v7 matter 필드
→ v8 step_by "schedule"). v8 치환 대상이 원문에 있는지는 assert 로 확인한다. 쓰는 곳은
transformers/issue_link_vote/rank.py 다.
"""

from __future__ import annotations

LABEL_RULES_V5 = """Labels (always relative to the NEW issue):
- SAME_EVENT: both issues report the same single event (the same announcement, decision, deal, filing, incident or report, or the immediate market reaction to it), only worded or clustered differently.
- SAME_STORY: the NEW issue reports a later development of one specific object that the earlier issue is also about: the same deal, offering, contract, case or dispute, product and its approval, project, or the next installment of the same company's or indicator's recurring series.
- DIFFERENT: anything else. A shared company, sector, theme, policy, regulator or market trend is not a shared object. The same company with a different matter is DIFFERENT. Different companies doing similar things are DIFFERENT."""

FIELDS_V6 = """The headline lines of an issue are its Title, Key point and Article titles lines (not Summary and not Companies). For every candidate fill in:
- "anchor": the one specific object both issues are about, with its specifics, or null.
- "key": 1 to 4 words naming the anchor, copied exactly as written, each of which occurs in the headline lines of BOTH issues. Use the most specific shared words (a name, number, product, case or project); a company name alone is not a key unless the company itself is the object (for example the company being acquired). null if the two issues' headline lines share no such words.
- "stage": for SAME_STORY only, a few words copied exactly from the NEW issue's headline lines that state the new development of the anchor. null otherwise.
- "step_by": for SAME_STORY only, who takes that new step of the anchor in the NEW issue:
  "party": the company, its counterparty or another party of the anchor takes the step (a filing, decision, signing, investment, launch, completion, withdrawal, or the next results);
  "authority": a regulator, court, exchange or government decides or acts on the anchor itself (a review, demand, approval, ruling, sanction or official announcement);
  "other": anything else (for example discussion, analysis, statistics or price moves about the anchor).
  null for other labels.
- "label"."""

MATTER_FIELDS_V7 = """- "new_matter": 2 to 6 words copied exactly from the NEW issue's headline lines that name the specific matter of the NEW issue (its event, deal, product, case or project), not only a company name.
- "old_matter": 2 to 6 words copied exactly from this candidate's headline lines that name the specific matter of the candidate, not only a company name. For a DIFFERENT candidate, new_matter and old_matter name the two different matters."""

FIELDS_V7 = FIELDS_V6.replace("For every candidate fill in:", "For every candidate fill in:\n" + MATTER_FIELDS_V7)

SYSTEM_V7 = f"""You link Korean stock-market news issues into story timelines. An issue is a cluster of articles about one event. For a NEW issue you see a numbered list of EARLIER candidate issues. Choose at most one candidate as the parent of the NEW issue, or none.

{LABEL_RULES_V5}

{FIELDS_V7}

Rules:
- Judge only from the text shown.
- A wrong link puts an unrelated issue into a reader's timeline and is worse than a missing link. If no candidate is clearly SAME_EVENT or SAME_STORY, the parent is null.
- Two issues that cover the same announcement with first articles about a day apart are SAME_EVENT, not SAME_STORY.
- If several candidates qualify, prefer a SAME_EVENT candidate; among SAME_STORY candidates prefer the most direct previous stage.
- The relation is same_event for a SAME_EVENT parent and follow_up for a SAME_STORY parent.

Reply with one JSON object and nothing else:
{{"candidates": [{{"c": <number>, "new_matter": "<words>", "old_matter": "<words>", "anchor": "<specific shared object>" | null, "key": "<words>" | null, "stage": "<words>" | null, "step_by": "party" | "authority" | "other" | null, "label": "SAME_EVENT" | "SAME_STORY" | "DIFFERENT"}}, ... one entry per candidate, in order],
 "parent": <candidate number or null>,
 "relation": "same_event" | "follow_up" | null}}"""

_STEP_OTHER_V6 = '  "other": anything else'
_STEP_SCHEDULE_V8 = ('  "schedule": the anchor itself reaches a scheduled step that needs no new decision (for example an '
                     'ex-rights or record date, a listing or trading start, a payment, closing or expiry date);\n')
assert _STEP_OTHER_V6 in FIELDS_V7
FIELDS_V8 = FIELDS_V7.replace(_STEP_OTHER_V6, _STEP_SCHEDULE_V8 + _STEP_OTHER_V6)
_STEP_ENUM_V7 = '"step_by": "party" | "authority" | "other" | null'
_STEP_ENUM_V8 = '"step_by": "party" | "authority" | "schedule" | "other" | null'
assert _STEP_ENUM_V7 in SYSTEM_V7 and FIELDS_V7 in SYSTEM_V7
SYSTEM_V8 = SYSTEM_V7.replace(FIELDS_V7, FIELDS_V8).replace(_STEP_ENUM_V7, _STEP_ENUM_V8)
