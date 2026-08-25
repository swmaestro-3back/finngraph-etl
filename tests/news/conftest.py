"""news 통합 테스트용 환경 주입.

`pipelines.news.config`는 import 시점에 다수의 env를 즉시 int/float로 캐스팅하므로,
CI처럼 `.env`가 없는 환경에서 news 모듈을 import하면 캐스팅에서 바로 실패한다. 로더
테스트가 필요로 하는 최소 값을 여기서 주입해 import가 통과하도록 한다.

`setdefault`이므로 CI job env나 실제 `.env`가 먼저 있으면 그 값이 우선한다.
DB_* 기본값은 로컬 `docker compose up -d db`의 노출 포트(15432)에 맞춘 것이고,
CI db-integration job은 서비스 컨테이너에 맞춰 DB_PORT 등을 env로 덮어쓴다.
"""

from __future__ import annotations

import os

_ENV_DEFAULTS = {
    # DB 접속 (news 로더는 psycopg(3)로 직접 연결)
    "DB_HOST": "localhost",
    "DB_PORT": "15432",
    "DB_NAME": "finngraph",
    "DB_USER": "threeback",
    "DB_PASSWORD": "12345678",
    # config import 시점에 캐스팅되는 상수. 로더 테스트와 무관하지만 import 통과에 필요.
    "SEARCH_DISPLAY": "1",
    "MAX_PAGES": "1",
    "OFFICIAL_SOURCE_THRESHOLD": "1",
    "REQUEST_DELAY": "1",
}

for _key, _value in _ENV_DEFAULTS.items():
    os.environ.setdefault(_key, _value)
