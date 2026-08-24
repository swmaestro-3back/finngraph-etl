"""OpenDART 공용 클라이언트.

비상장 개요·재무와 기업 설명 생성이 같은 인증키·레이트리밋을 쓰므로 common에 둔다.

DART는 실패를 HTTP 상태가 아니라 응답 본문의 `status` 필드로 알려준다. 특히 `013`
(조회된 데이터가 없습니다)은 오류가 아니라 정상적인 빈 결과다 — 비상장사는 재무를 아예
제출하지 않는 해가 흔하다. 이를 예외로 다루면 배치가 첫 결측에서 죽으므로 구분한다.
"""

from __future__ import annotations

import io
import json
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
            # 오류일 때는 zip 대신 XML/JSON 본문이 온다. status/message 를 최대한 살려
            # 호출자가 013/014(없음, 건너뜀)와 020/021(한도, 중단)을 구분하게 한다.
            status, message = _extract_error(content)
            raise DartApiError(status, message or f"zip 응답이 아님: {content[:200]!r}") from exc

    def get_document_text(self, rcept_no: str) -> str:
        """공시 원본(document.xml)에서 본문 XML을 골라 문자열로 돌려준다.

        zip 안에 XML이 여러 개 들어 있고 **접미사 없는 `{rcept_no}.xml`이 본문**이다.
        나머지는 첨부·감사보고서다. 규칙이 어긋나는 공시도 있어, 정확히 일치하는 이름이
        없으면 가장 큰 XML을 본문으로 본다(첨부보다 본문이 크다).

        Args:
            rcept_no (str): 접수번호.

        Returns:
            str: 본문 문자열. zip에 XML이 없으면 빈 문자열.
        """

        members = self.get_zip_members("document.xml", {"rcept_no": rcept_no})
        xml_members = {
            name: content for name, content in members.items() if name.lower().endswith(".xml")
        }
        if not xml_members:
            return ""

        exact = f"{rcept_no}.xml"
        if exact in xml_members:
            content = xml_members[exact]
        else:
            name, content = max(xml_members.items(), key=lambda item: len(item[1]))
            logger.info(
                "본문 XML 이름 규칙 불일치, 최대 크기 파일 사용: rcept_no=%s file=%s",
                rcept_no,
                name,
            )

        # 공시 원본은 EUC-KR과 UTF-8이 섞여 있다. 선언을 신뢰하지 않고 순서대로 시도한다.
        for encoding in ("utf-8", "cp949"):
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        return content.decode("utf-8", errors="replace")

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


def _extract_error(content: bytes) -> tuple[str, str]:
    """zip이 아닌 오류 본문에서 (status, message)를 뽑는다. 못 뽑으면 빈 문자열."""

    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return "", ""
    if not isinstance(payload, dict):
        return "", ""
    return str(payload.get("status", "")), str(payload.get("message", ""))


def get_dart_client() -> DartClient:
    """job에서 쓰는 기본 클라이언트."""

    return DartClient()
