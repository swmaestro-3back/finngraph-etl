"""한국투자증권(KIS) Open API 공용 클라이언트.

시세·재무·수급/배당이 모두 같은 인증과 레이트리밋을 쓰므로 도메인 패키지가 아니라
common에 둔다. 도메인별 extractor는 이 클라이언트 위에서
엔드포인트와 응답 파싱만 담당한다.

접근토큰 두 가지 제약이 설계를 결정한다.

1. 유효기간이 24시간이다.
2. **발급 자체에 분당 1회 제한**이 있다. job마다 새로 발급하면 여러 DAG가 같은 시각에
   돌 때 곧바로 한도에 걸린다.

그래서 토큰을 캐시해 프로세스 간에 공유한다. Airflow는 태스크마다 별개 프로세스라
메모리 캐시로는 부족하다.

**저장소는 `TokenStore`로 갈아끼운다.** 기본 구현(`FileTokenStore`)은 로컬 파일이라
**같은 파일시스템을 공유하는 실행 환경(LocalExecutor·단일 호스트)을 전제한다.** 태스크가
분산 실행되면(KubernetesExecutor 등 Pod마다 별개 `/tmp`) 캐시가 공유되지 않아 Pod 수만큼
발급을 시도하고 분당 1회 제한에 걸린다. 그때는 공유 저장소 구현으로 교체한다.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import requests

from pipelines.common.config import Settings, get_settings
from pipelines.common.logging import get_logger
from pipelines.common.rate_limit import FixedWindowRateLimiter
from pipelines.common.retry import retry_external_call

logger = get_logger(__name__)

TOKEN_PATH = "/oauth2/tokenP"

# 만료 직전에 쓰다 401을 맞지 않도록 남은 수명이 이보다 짧으면 새로 발급한다.
TOKEN_EXPIRY_MARGIN_SECONDS = 600

# 토큰 발급 분당 1회 제한에 걸렸을 때 재시도까지 기다리는 시간.
TOKEN_ISSUE_COOLDOWN_SECONDS = 65


@dataclass(frozen=True)
class CachedToken:
    """저장소에 오가는 토큰 한 벌.

    Attributes:
        access_token (str): 접근토큰.
        expires_at (float): 만료 시각(epoch 초).
        app_key (str): 발급에 쓴 앱키. 운영↔모의 전환을 감지하는 데 쓴다.
    """

    access_token: str
    expires_at: float
    app_key: str


class TokenStore(Protocol):
    """접근토큰 저장소.

    유효성 판정(만료 여유·앱키 일치)은 클라이언트가 하고, 저장소는 보관과 발급 직렬화만
    맡는다. 그래야 백엔드를 바꿔도 정책이 흩어지지 않는다.
    """

    def read(self) -> CachedToken | None:
        """저장된 토큰. 없거나 읽지 못하면 None."""

    def write(self, token: CachedToken) -> None:
        """토큰을 저장한다. 실패해도 예외를 던지지 않는다 — 캐시는 최적화일 뿐이다."""

    def issue_lock(self) -> AbstractContextManager[None]:
        """발급 구간을 감싸는 락.

        분당 1회 제한 때문에 동시 발급은 대부분 실패한다. 공유 저장소 구현은 여기서 실제
        락을 잡아 전체에서 1회만 발급되게 만든다. 락이 없는 구현은 빈 컨텍스트를 준다.
        """


class FileTokenStore:
    """로컬 파일 기반 저장소.

    **같은 파일시스템을 공유하는 실행 환경을 전제한다.** 원자적 교체로 "반쯤 쓰인 파일을
    읽는" 문제는 막지만, 프로세스 간 락이 없어 **동시 발급 자체는 막지 못한다.** 동시성이
    낮은 단일 호스트 실행에서만 충분하다.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def read(self) -> CachedToken | None:
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

        # 필드가 빠졌거나 타입이 다르면 손상된 레코드다. 만료 시각을 0으로 채워 넘기면
        # 클라이언트가 만료로 걸러내긴 하지만, 저장소가 온전하지 않은 값을 만들지 않는다.
        token = raw.get("access_token")
        expires_at = raw.get("expires_at")
        if not token or not isinstance(expires_at, int | float):
            return None

        return CachedToken(
            access_token=str(token),
            expires_at=float(expires_at),
            app_key=str(raw.get("app_key", "")),
        )

    def write(self, token: CachedToken) -> None:
        payload = {
            "access_token": token.access_token,
            "expires_at": token.expires_at,
            "app_key": token.app_key,
        }

        # 여러 job이 동시에 발급할 수 있다. 임시 파일에 쓰고 원자적으로 바꿔치기해
        # 반쯤 쓰인 파일을 다른 프로세스가 읽는 일이 없게 한다.
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                delete=False,
            ) as handle:
                json.dump(payload, handle)
                temp_path = handle.name
            os.replace(temp_path, self._path)
            os.chmod(self._path, 0o600)
        except OSError as exc:
            # 캐시는 최적화일 뿐이라 실패해도 호출 자체는 계속되어야 한다.
            logger.warning("KIS 토큰 캐시 기록 실패 (%s): %s", self._path, exc)

    def issue_lock(self) -> AbstractContextManager[None]:
        # 파일 저장소에는 프로세스 간 락이 없다. 동시 발급은 분당 1회 제한에 걸리고,
        # 클라이언트의 쿨다운 재시도가 흡수한다.
        return nullcontext()


class KisApiError(RuntimeError):
    """KIS가 HTTP 200과 함께 실패 코드(rt_cd != '0')를 돌려준 경우.

    Attributes:
        rt_cd (str): 응답 코드. '0'이 정상이다.
        msg_cd (str): 상세 메시지 코드.
        msg (str): 사람이 읽는 메시지.
    """

    def __init__(self, rt_cd: str, msg_cd: str, msg: str) -> None:
        super().__init__(f"KIS API error rt_cd={rt_cd} msg_cd={msg_cd} msg={msg}")
        self.rt_cd = rt_cd
        self.msg_cd = msg_cd
        self.msg = msg


class KisClient:
    """KIS Open API 호출 클라이언트.

    인증(토큰 발급·캐시), 레이트리밋, 실패 코드 판정을 담당한다. 엔드포인트별 파라미터와
    응답 해석은 호출하는 extractor의 몫이다.
    """

    def __init__(
        self, settings: Settings | None = None, token_store: TokenStore | None = None
    ) -> None:
        self._settings = settings or get_settings()
        self._base_url = self._settings.kis_base_url.rstrip("/")
        self._store = token_store or FileTokenStore(self._settings.kis_token_cache_path)
        self.rate_limiter = FixedWindowRateLimiter(self._settings.kis_rate_limit_per_second)
        self._session = requests.Session()

    # -- 요청 -------------------------------------------------------------

    def request(
        self,
        path: str,
        tr_id: str,
        params: dict[str, str],
        *,
        custtype: str = "P",
        timeout: int = 30,
    ) -> dict[str, Any]:
        """GET 요청을 보내고 정상 응답(JSON)을 반환한다.

        Args:
            path (str): '/uapi/...' 형태의 엔드포인트 경로.
            tr_id (str): 거래 ID. 엔드포인트마다 다르다.
            params (dict[str, str]): 쿼리 파라미터.
            custtype (str): 고객 타입. 개인은 'P'.
            timeout (int): 요청 타임아웃(초).

        Returns:
            dict[str, Any]: 응답 JSON 전체.

        Raises:
            KisApiError: rt_cd가 '0'이 아닌 경우.
            requests.HTTPError: HTTP 상태가 실패인 경우.
        """

        self.rate_limiter.wait()
        data = self._get(path, tr_id, params, custtype, timeout)

        rt_cd = str(data.get("rt_cd", ""))
        if rt_cd != "0":
            raise KisApiError(rt_cd, str(data.get("msg_cd", "")), str(data.get("msg1", "")).strip())

        return data

    @retry_external_call()
    def _get(
        self,
        path: str,
        tr_id: str,
        params: dict[str, str],
        custtype: str,
        timeout: int,
    ) -> dict[str, Any]:
        response = self._session.get(
            f"{self._base_url}{path}",
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {self.access_token()}",
                "appkey": self._settings.kis_app_key,
                "appsecret": self._settings.kis_app_secret,
                "tr_id": tr_id,
                "custtype": custtype,
            },
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    # -- 토큰 -------------------------------------------------------------

    def access_token(self) -> str:
        """유효한 접근토큰을 반환한다. 캐시가 살아 있으면 재사용한다."""

        cached = self._valid_token()
        if cached is not None:
            return cached

        with self._store.issue_lock():
            # 락을 기다리는 동안 다른 실행자가 발급했을 수 있다. 락 안에서 한 번 더 본다.
            cached = self._valid_token()
            if cached is not None:
                return cached
            return self._issue_token()

    def _valid_token(self) -> str | None:
        """저장된 토큰이 지금 쓸 수 있으면 반환한다."""

        cached = self._store.read()
        if cached is None:
            return None
        if cached.expires_at - TOKEN_EXPIRY_MARGIN_SECONDS <= time.time():
            return None
        # 캐시는 앱키 단위로 유효하다. 키가 바뀌면(운영↔모의) 남은 토큰을 쓰면 안 된다.
        if cached.app_key != self._settings.kis_app_key:
            return None

        return cached.access_token

    def _issue_token(self) -> str:
        data = self._post_token()

        if "access_token" not in data:
            # 분당 1회 제한(EGW00133)에 걸린 경우다. 다른 프로세스가 방금 발급했을 수 있으니
            # 잠시 기다렸다가 캐시를 한 번 더 본 뒤, 그래도 없으면 한 번만 재시도한다.
            logger.warning(
                "KIS 토큰 발급 실패, %d초 후 재시도: %s", TOKEN_ISSUE_COOLDOWN_SECONDS, data
            )
            time.sleep(TOKEN_ISSUE_COOLDOWN_SECONDS)

            cached = self._valid_token()
            if cached is not None:
                return cached

            data = self._post_token()

        if "access_token" not in data:
            raise KisApiError(
                str(data.get("error_code", "")),
                "",
                str(data.get("error_description", data)),
            )

        token = str(data["access_token"])
        expires_in = int(data.get("expires_in", 86400))
        self._store.write(
            CachedToken(
                access_token=token,
                expires_at=time.time() + expires_in,
                app_key=self._settings.kis_app_key,
            )
        )
        logger.info("KIS 접근토큰 발급 완료 (유효 %d초)", expires_in)
        return token

    def _post_token(self) -> dict[str, Any]:
        response = self._session.post(
            f"{self._base_url}{TOKEN_PATH}",
            json={
                "grant_type": "client_credentials",
                "appkey": self._settings.kis_app_key,
                "appsecret": self._settings.kis_app_secret,
            },
            timeout=30,
        )
        try:
            return response.json()
        except ValueError:
            return {"error_description": response.text[:200]}


def get_kis_client() -> KisClient:
    """job에서 쓰는 기본 클라이언트."""

    return KisClient()
