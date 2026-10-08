"""이슈 연결 pair 투표자 프롬프트다.

screen(1차 거르기) → route 1(evidence 관점 프롬프트와 matter 관점 프롬프트, 각 3회 호출) →
route 2(A 의 계획 추출 → plan step → evidence v3) 순서로 쓴다(transformers/issue_link_vote/pair.py).
도구 호출용 프롬프트(EVIDENCE_TOOL_PROMPT, EVIDENCE2_TOOL_PROMPT, EVIDENCE3_TOOL_PROMPT)는
EVIDENCE_PROMPT 원문에 문자열 치환을 적용해 만든다. EVIDENCE3_TOOL_PROMPT 를 만들 때는 치환
대상이 정확히 한 번 나오는지 assert 로 확인한다. 프롬프트를 바꾸면
tests/news/test_issue_link_vote_prompts.py 의 해시 검사가 실패하므로, 해시와 llm.PROMPT_VERSIONS 를
함께 올린다.
"""

from __future__ import annotations

from typing import Any

# ── screen ──

SCREEN_PROMPT = """You maintain story timelines for a Korean stock-market news app. Each "issue" is a cluster of news articles about one event. The app links a later issue to an earlier issue only when a reader following the earlier issue would clearly want to see the later one as part of the same story. A wrong link drops an unrelated issue into someone's timeline, which is worse than a missing link.

You get two issues: A started earlier, B started later. Classify B's relation to A with exactly one label.

SAME_EVENT: A and B report the same single event. It is the same announcement, disclosure, decision, report, approval or incident, possibly told from different angles, and possibly with the immediate market reaction to that same event. This usually happens within a few days.

SAME_STORY: B is a later stage of the same concrete storyline as A. The storyline must be anchored on one specific thing that both issues are about, such as:
- the same deal, transaction, capital raise, merger, tender offer or filing (decision -> regulator review or amendment -> pricing, ex-rights, payment, result);
- the same drug or product going through the same approval or development process (review milestone -> approval);
- the same project, program or plan (talks or plan -> official agreement or announcement; consortium or MOU -> joint venture, investment or contract for that same project; a program -> a company's concrete role in that same program);
- the same lawsuit, investigation, sanction or labor dispute (action -> court ruling, appeal, settlement);
- something A explicitly announced, promised or was waiting for, which B reports;
- a recurring series about the same subject: the same company's results for the next period (preview -> results, or one quarter -> the next quarter), or the next release of the same economic indicator.

DIFFERENT: everything else. In particular:
- Different companies doing the same kind of thing (two listings, two reverse splits, two mergers, two capital raises, two companies' earnings or contracts). The same event type is not the same story.
- The same company, but a different deal, counterparty, product, drug, plant, project or business line. Two separate contracts, MOUs or investments of one company are DIFFERENT.
- Unrelated corporate actions of the same company (for example earnings versus a buyback, a new contract or an investment), unless A explicitly announced or set up what B reports.
- Overlap only in a theme, sector, policy trend, business group (for example affiliates of the same group) or buzzword.
- Analyst target prices, market commentary or stock moves that share only a theme or a company with a real event in the other issue.

How to decide:
1. Name the specific subject of A and of B: who did what, with whom, about which deal, product, project or dispute.
2. Ask whether both subjects are the same concrete thing (or a recurring series of the same subject). Company tags can be noisy because roundup articles tag many companies, so judge from the title, summary and article titles.
3. If they share the subject, choose SAME_EVENT when B reports the same occurrence as A, and SAME_STORY when B is a later development.
4. If you are unsure whether they share a concrete subject, answer DIFFERENT.

Reply with one JSON object and nothing else:
{"a_subject": "<A's specific subject in a few words>", "b_subject": "<B's specific subject in a few words>", "shared_subject": "<the concrete thing both are about, or none>", "label": "SAME_EVENT" | "SAME_STORY" | "DIFFERENT", "reason": "<one sentence>"}"""

# ── evidence 관점 프롬프트 원문. 도구 호출로 답하게 바꾼 것이 EVIDENCE_TOOL_PROMPT 다 ──

EVIDENCE_PROMPT = """You audit a story timeline for a Korean stock-market news app. Each "issue" is a cluster of news articles about one event. A proposed link says that the later issue B continues the story of the earlier issue A. A wrong link drops an unrelated issue into someone's timeline, which is worse than a missing link. Accept a link only when the texts themselves show that A and B are about the same specific matter.

The same specific matter is one of these, named in both issues:
- the same occurrence: the same announcement, disclosure, decision, report, approval or incident (often on the same or nearby dates);
- the same transaction, offering, merger, tender offer, filing, contract or investment (same company, same counterparty or same terms);
- the same drug or product in the same approval or development process;
- the same named project, program or plan, including talks about it and its official announcement;
- the same lawsuit, investigation, sanction or labor dispute;
- a pending step that A names explicitly and B reports;
- a recurring series of the same subject: the same company's results for consecutive periods (preview -> results, or one quarter -> the next), or consecutive releases of the same economic indicator.

These do NOT show the same specific matter:
- the same company alone, or companies of the same business group or sector;
- the same type of event at different companies;
- a shared market trend or theme (rising prices, a shortage, a sector rally, a policy theme);
- a broad goal or strategy in A (expand business, use of proceeds, shareholder returns, capacity growth) and a separate later action in B;
- a government policy, partnership or regulation in A and one company's own deal or situation in B, unless B is about the same named plan or project itself;
- a stock move, analyst note or market roundup on one side.

Company tags can be noisy because roundup articles tag many companies. Read the titles, summaries and article titles.

Steps:
1. Quote or closely paraphrase the words in A that name its specific matter.
2. Quote or closely paraphrase the words in B that name the same matter. If B never refers to A's matter (and A does not name B's step as pending), there is no evidence.
3. Decide. SAME_EVENT: same occurrence. SAME_STORY: B is a later development of the same specific matter, or the next installment of the same series. DIFFERENT: no such evidence, or you are unsure.

Reply with one JSON object and nothing else:
{"matter_in_a": "<words from A>", "matter_in_b": "<words from B, or none>", "same_specific_matter": true | false, "label": "SAME_EVENT" | "SAME_STORY" | "DIFFERENT", "reason": "<one sentence>"}"""

# ── route 1: evidence 관점 프롬프트(도구 호출)와 matter 관점 프롬프트 ──

_EVIDENCE_REPLY = EVIDENCE_PROMPT[EVIDENCE_PROMPT.index("Reply with one JSON object"):]
EVIDENCE_TOOL_PROMPT = EVIDENCE_PROMPT.replace(_EVIDENCE_REPLY, "Record your verdict with the record_verdict tool.")

EVIDENCE_TOOL: dict[str, Any] = {
    "name": "record_verdict",
    "description": "Record the verdict on whether issue B continues the story of issue A.",
    "schema": {
        "type": "object",
        "properties": {
            "matter_in_a": {"type": "string", "description": "Words from A that name its specific matter."},
            "matter_in_b": {"type": "string", "description": "Words from B that name the same matter, or 'none'."},
            "same_specific_matter": {"type": "boolean"},
            "label": {"type": "string", "enum": ["SAME_EVENT", "SAME_STORY", "DIFFERENT"]},
            "reason": {"type": "string", "description": "One sentence."},
        },
        "required": ["matter_in_a", "matter_in_b", "same_specific_matter", "label", "reason"],
    },
}

MATTER_BASES = ("same_occurrence", "specific_step", "broad_step", "instance", "background_only", "theme_only", "none")
MATTER_ACCEPTED = frozenset({"same_occurrence", "specific_step", "broad_step"})

MATTER_PROMPT = """You audit a story timeline for a Korean stock-market news app. Each "issue" is a cluster of news articles about one event. A proposed link says that the later issue B continues the story of the earlier issue A. A wrong link drops an unrelated issue into someone's timeline, which is worse than a missing link.

B's own news is the development that B's title, headline point and article titles report. What B's summary restates about earlier events, rules, programs or plans is background.

Step 1. Name the matter of A through which B would continue A's story, in A's own words.

Step 2. Classify that matter.
- specific: one concrete matter: one transaction, offering, merger, tender offer, filing, contract or investment with named parties; one drug or product in one approval process; one lawsuit, sanction, investigation or labor dispute; one named project of a company; one occurrence (an announcement, disclosure, decision, report or incident); or a recurring series of the same subject (one company's results by period, one economic indicator by release).
- broad: a government policy, regulation, rule or national program; talks or agreements between governments; market-wide statistics or a roundup of many companies; a sector trend, theme or market rally; a stock move on outside news; or a company's broad goal, pledge or strategy without a concrete transaction.

Step 3. Decide from B's own news.
- For a specific matter, B continues A when B's own news is the same occurrence, a new step of that matter (review, amendment, pricing, payment, ruling, approval, failure, completion, official agreement), the step that A names as pending, or the next installment of the series.
- For a broad matter, B continues A only when B's own news is the same occurrence, or a step of the broad matter as a whole: the same policy, rule, program, plan or talks being officially announced, agreed, signed, enacted, revised or ruled on, or the next release of the same statistics. One company's own case, deal, filing, project, result or action that falls under the policy, program, theme or goal does not continue A, even when B's summary describes the broad matter.

Bases:
- same_occurrence: B's own news is the same occurrence as A's, including the market reaction to it in the same or other stocks.
- specific_step: a new step of A's specific matter, A's named pending step, or the next installment of the series.
- broad_step: a step of A's broad matter as a whole.
- instance: B's own news is one company's own case, deal or action under A's broad matter.
- background_only: A's matter appears in B only as background, and B's own news is something else.
- theme_only: A and B share only a company, a business group, a sector, a theme, a policy area or a type of event.
- none: nothing in common.

Company tags can be noisy because roundup articles tag many companies.

Fill the fields in order. Then choose the label: SAME_EVENT for same_occurrence, SAME_STORY for specific_step or broad_step, and DIFFERENT for every other basis. If you are unsure, choose DIFFERENT."""

MATTER_TOOL: dict[str, Any] = {
    "name": "record_verdict",
    "description": "Record the verdict on whether issue B continues the story of issue A.",
    "schema": {
        "type": "object",
        "properties": {
            "a_matter": {"type": "string", "description": "The matter of A through which B would continue A."},
            "matter_scope": {"type": "string", "enum": ["specific", "broad"]},
            "b_own_news": {"type": "string", "description": "B's own news in one short sentence."},
            "basis": {"type": "string", "enum": list(MATTER_BASES)},
            "label": {"type": "string", "enum": ["SAME_EVENT", "SAME_STORY", "DIFFERENT"]},
            "reason": {"type": "string", "description": "One sentence."},
        },
        "required": ["a_matter", "matter_scope", "b_own_news", "basis", "label", "reason"],
    },
}

# ── route 2: evidence v2·v3 ──

_EV_OLD_STEPS = EVIDENCE_PROMPT[EVIDENCE_PROMPT.index("Steps:"):]
_EV_NEW_STEPS = """Steps:
1. List the specific matters that A names: the matter that A's title, headline point and article titles report, and each concrete plan, project or pending step that A's title, headline point or summary states for a named company or named parties, with a named object (a product, project name, plant or site, country or region of operation, counterparty, amount, or dated next step). A broad goal without a named object, another party's remark or expectation about a company, and market conditions are not concrete plans. Quote or closely paraphrase A's words.
2. Quote or closely paraphrase the words in B's title, headline point or article titles that name one of those matters. B's summary is background: a matter that appears in B only in its summary is not evidence. If B's own words never refer to any of A's matters, there is no evidence.
3. Decide. SAME_EVENT: same occurrence. SAME_STORY: B is a later development of one of A's specific matters (the same company or named parties carry out, advance, change or end it), or the next installment of the same series. DIFFERENT: no such evidence, or you are unsure.

Record your verdict with the record_verdict tool."""

EVIDENCE2_TOOL_PROMPT = EVIDENCE_PROMPT.replace(_EV_OLD_STEPS, _EV_NEW_STEPS).replace(
    "- a pending step that A names explicitly and B reports;",
    "- a pending step, or a concrete plan or project, that A states explicitly and B reports being carried out;",
)

EVIDENCE2_TOOL: dict[str, Any] = {
    "name": "record_verdict",
    "description": "Record the verdict on whether issue B continues the story of issue A.",
    "schema": {
        "type": "object",
        "properties": {
            "matters_in_a": {"type": "string", "description": "A's specific matters, each in A's words, separated by ' | '."},
            "matter_in_b": {
                "type": "string",
                "description": "Words from B's title, headline point or article titles that name one of A's matters, or 'none'.",
            },
            "same_specific_matter": {"type": "boolean"},
            "label": {"type": "string", "enum": ["SAME_EVENT", "SAME_STORY", "DIFFERENT"]},
            "reason": {"type": "string", "description": "One sentence."},
        },
        "required": ["matters_in_a", "matter_in_b", "same_specific_matter", "label", "reason"],
    },
}

_EXPLICIT = ("The connection must be explicit in the two texts. Do not connect them through outside knowledge: "
             "a project, product, site or plan that A's text does not name is not one of A's matters or plans, "
             "even if you know that it is related.")

_EV_PROJECT = ("- one company project carried out through several transactions: an MOU, consortium or plan for a "
               "project, then a joint venture, investment, plant or contract for that same project, even with "
               "another counterparty, when both issues name the same product, project or site;")
_EV_ANCHOR = "- the same drug or product in the same approval or development process;"
EVIDENCE3_TOOL_PROMPT = EVIDENCE2_TOOL_PROMPT
for _old, _new in ((_EV_ANCHOR, f"{_EV_ANCHOR}\n{_EV_PROJECT}"),
                   ("Company tags can be noisy because roundup articles tag many companies. Read the titles, summaries and article titles.",
                    f"{_EXPLICIT}\n\nCompany tags can be noisy because roundup articles tag many companies. Read the titles, summaries and article titles.")):
    assert EVIDENCE3_TOOL_PROMPT.count(_old) == 1, _old
    EVIDENCE3_TOOL_PROMPT = EVIDENCE3_TOOL_PROMPT.replace(_old, _new)
EVIDENCE3_TOOL = EVIDENCE2_TOOL

# ── route 2: A 하나만 읽는 계획 추출과 plan step ──

PLAN_EXTRACT_PROMPT = """You read one issue (a cluster of Korean stock-market news articles) and list the concrete plans it states.

A concrete plan is a project, plan or pending step that the issue's title, headline point or summary states for a named company or named parties: something they are doing or will do, with a named object such as a product, a project name, a plant or site, a country or region of operation, a counterparty, an amount, or a dated next step (a filing, payment, signing, ruling, approval or decision date).

These are not concrete plans:
- a broad goal, pledge or strategy without a named object (grow a business, improve profitability, return more to shareholders);
- another party's remark, praise, forecast or expectation about a company, or expected benefits for a sector;
- market conditions, stock moves and analyst views;
- background about other companies;
- the event that the issue itself reports as done (it is the issue's own news, not a plan).

Use only what the text states. Do not add details from outside knowledge. Most issues state no concrete plan; then return an empty list.

Record the plans with the tool."""

PLAN_EXTRACT_TOOL: dict[str, Any] = {
    "name": "record_plans",
    "description": "Record the concrete plans that the issue states.",
    "schema": {
        "type": "object",
        "properties": {
            "plans": {
                "type": "array",
                "maxItems": 4,
                "items": {
                    "type": "object",
                    "properties": {
                        "who": {"type": "string", "description": "The company or parties that carry out the plan."},
                        "plan": {"type": "string", "description": "The plan in the issue's own words."},
                        "named_object": {
                            "type": "string",
                            "description": "The product, project name, site, region or counterparty, copied exactly as written in the issue.",
                        },
                    },
                    "required": ["who", "plan", "named_object"],
                },
            },
        },
        "required": ["plans"],
    },
}

PLANSTEP_RELATIONS = ("carries_out", "market_outcome", "other_party", "different_matter", "summary_only", "none")

PLANSTEP_PROMPT = """You audit a story timeline for a Korean stock-market news app. Each "issue" is a cluster of news articles about one event. A wrong link drops an unrelated issue into someone's timeline, which is worse than a missing link.

The earlier issue A states one or more concrete plans. They were listed from A alone and are given below A's card. Decide whether the later issue B continues A's story because B's own news carries out one of those plans.

B's own news is the development that B's title, headline point and article titles report. What B's summary restates is background.

B carries out a plan when B's own news reports that the same company or named parties (the plan's "who") do, advance, change, complete or abandon that very plan, for example a contract, investment, joint venture, filing, payment, approval, construction or launch for it, and B's own news names the plan's object (the same product, project, site or counterparty).

B does not carry out a plan when:
- market_outcome: B's own news is a stock move, investor flows, a supply-demand outlook, a price trend or an analyst view;
- other_party: B's own news is an action of a party other than the plan's "who", including one company's own case under a government plan, rule or program;
- different_matter: B's own news is a different project, product, site, transaction or program of the same company that the plan does not name;
- summary_only: the plan's object appears in B only in B's summary;
- none: nothing in common.

The connection must be explicit in the two texts. Do not connect them through outside knowledge. Company tags can be noisy because roundup articles tag many companies.

Fill the fields in order. b_words must be copied exactly, as one contiguous phrase, from B's title, headline point or one article title. Choose SAME_STORY only for carries_out; otherwise choose DIFFERENT. If you are unsure, choose DIFFERENT."""

PLANSTEP_TOOL: dict[str, Any] = {
    "name": "record_verdict",
    "description": "Record whether B's own news carries out one of A's listed plans.",
    "schema": {
        "type": "object",
        "properties": {
            "b_own_news": {"type": "string", "description": "B's own news in one short sentence."},
            "plan_index": {"type": "integer", "description": "1-based index of the plan that B carries out, or 0."},
            "b_words": {
                "type": "string",
                "description": "Exact words from B's title, headline point or one article title that name the plan's object, or ''.",
            },
            "relation": {"type": "string", "enum": list(PLANSTEP_RELATIONS)},
            "label": {"type": "string", "enum": ["SAME_STORY", "DIFFERENT"]},
            "reason": {"type": "string", "description": "One sentence."},
        },
        "required": ["b_own_news", "plan_index", "b_words", "relation", "label", "reason"],
    },
}
