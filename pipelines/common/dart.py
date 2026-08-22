"""OpenDART 공용 클라이언트.

비상장 개요·재무와 기업 설명 생성이 같은 인증키·레이트리밋을 쓰므로 common에 둔다.

DART는 실패를 HTTP 상태가 아니라 응답 본문의 `status` 필드로 알려준다. 특히 `013`
(조회된 데이터가 없습니다)은 오류가 아니라 정상적인 빈 결과다 — 비상장사는 재무를 아예
제출하지 않는 해가 흔하다. 이를 예외로 다루면 배치가 첫 결측에서 죽으므로 구분한다.
"""

from __future__ import annotations

import io
import zipfile
from typing import Any

import requests

from pipelines.common.config import Settings, get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.rate_limit import FixedWindowRateLimiter
from pipelines.common.utils.retry import retry_external_call

logger = get_logger(__name__)

STATUS_OK = "000"
STATUS_NO_DATA = "013"


class DartApiError(RuntimeError):
    """OpenDART가 정상(000)이 아닌 status를 돌려준 경우."""

    def __init__(self, status: str, message: str) -> None:
        super().__init__(f"DART API error status={status} message={message}")
        self.status = status
        self.message = message


class DartClient:
    """OpenDART REST 클라이언트."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._base_url = self._settings.dart_base_url.rstrip("/")
        self.rate_limiter = FixedWindowRateLimiter(self._settings.dart_rate_limit_per_second)
        self._session = requests.Session()

    def get_json(
        self,
        path: str,
        params: dict[str, str] | None = None,
        *,
        allow_no_data: bool = True,
        timeout: int = 60,
    ) -> dict[str, Any]:
        """JSON 엔드포인트를 호출한다.

        Args:
            path (str): 'company.json' 같은 엔드포인트 이름.
            params (dict[str, str] | None): 인증키를 제외한 쿼리 파라미터.
            allow_no_data (bool): status가 013(데이터 없음)일 때 예외 대신 빈 dict를 반환할지.
            timeout (int): 요청 타임아웃(초).

        Returns:
            dict[str, Any]: 응답 JSON. 데이터가 없고 allow_no_data면 빈 dict.

        Raises:
            DartApiError: status가 000도 013도 아닌 경우.
        """

        self.rate_limiter.wait()
        data = self._get_json(path, params or {}, timeout)

        status = str(data.get("status", ""))
        if status == STATUS_OK:
            return data
        if status == STATUS_NO_DATA and allow_no_data:
            return {}

        raise DartApiError(status, str(data.get("message", "")))

    def get_zip_members(
        self,
        path: str,
        params: dict[str, str] | None = None,
        *,
        timeout: int = 120,
    ) -> dict[str, bytes]:
        """zip으로 내려오는 엔드포인트(corpCode.xml, document.xml)를 풀어 반환한다.

        Args:
            path (str): 엔드포인트 이름.
            params (dict[str, str] | None): 인증키를 제외한 쿼리 파라미터.
            timeout (int): 요청 타임아웃(초).

        Returns:
            dict[str, bytes]: zip 내부 파일 이름 → 내용.

        Raises:
            DartApiError: zip이 아니라 오류 JSON/XML이 돌아온 경우.
        """

        self.rate_limiter.wait()
        content = self._get_bytes(path, params or {}, timeout)

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                return {name: archive.read(name) for name in archive.namelist()}
        except zipfile.BadZipFile as exc:
            # 오류일 때는 zip 대신 XML/JSON 본문이 온다.
            raise DartApiError("", f"zip 응답이 아님: {content[:200]!r}") from exc

    @retry_external_call()
    def _get_json(self, path: str, params: dict[str, str], timeout: int) -> dict[str, Any]:
        response = self._session.get(
            f"{self._base_url}/{path}",
            params={"crtfc_key": self._settings.dart_api_key, **params},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    @retry_external_call()
    def _get_bytes(self, path: str, params: dict[str, str], timeout: int) -> bytes:
        response = self._session.get(
            f"{self._base_url}/{path}",
            params={"crtfc_key": self._settings.dart_api_key, **params},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.content


def get_dart_client() -> DartClient:
    """job에서 쓰는 기본 클라이언트."""

    return DartClient()
