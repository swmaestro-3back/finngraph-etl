"""이슈 연결 투표 판정이다. 투표자 넷이 대상 이슈 B 와 앞선 후보 A 쌍마다 통과 여부를 낸다.

  pair    제안자(pair.py)       screen → route 1(evidence·matter 관점 프롬프트) → route 2(계획 경로)
  rank    제안자(rank.py)       후보 목록을 한 번에 보는 호출(rank-v8) + check_affirm 코드 점검
  judge   확인자(confirm.py)    scope 판정(scope-v4), 실패하면 계획 이행 확인(plan-v5)과 코드 점검
  check   확인자(confirm.py)    scope 점검(scopecheck-v2)

screen 은 모든 쌍을 먼저 거르는 1차 거르기 단계다. judge 표는 scope 판정으로 정하고, scope 판정이
실패하면 계획 이행 확인 결과로 정한다. judge AND check AND (pair OR rank) 일 때만 잇는다(decide.py).
주가 반응 이슈는 연결에서 모두 뺀다(kind.py). LLM 호출의 라우팅·검증·캐시·상한은 llm.py, 부모 순위
점수는 views.py 에 있다. 흐름과 DB 접근은 jobs/link_issues.py 가 맡는다.
"""
