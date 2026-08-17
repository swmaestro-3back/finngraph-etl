from __future__ import annotations

import aiohttp


class AsyncHTTPClient:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    def start(self) -> None:
        """
        애플리케이션 시작 시 딱 한 번 호출하여 커넥션 풀(세션)을 생성한다.
        """
        if self._session is None:
            connector = aiohttp.TCPConnector(
                limit=100,
                ttl_dns_cache=300,
                keepalive_timeout=30,
            )
            self._session = aiohttp.ClientSession(connector=connector)

    async def stop(self) -> None:
        """
        애플리케이션(또는 스크립트) 종료 시 안전하게 커넥션 풀을 닫는다.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def get_session(self) -> aiohttp.ClientSession:
        """
        공통 세션을 반환한다.
        """
        if self._session is None:
            raise RuntimeError("HTTP Client가 시작되지 않았습니다. start()를 먼저 호출하세요.")
        return self._session

    # Airflow에서는 태스크마다 별개 프로세스라 세션을 태스크 단위로 만들고 닫아야 한다.
    # 실행 단위 진입점에서 `async with http_client:`로 감싸 수명을 관리한다.
    async def __aenter__(self) -> AsyncHTTPClient:
        self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()


# 전역적으로 하나의 객체만 사용
# 싱글톤 패턴 적용
http_client = AsyncHTTPClient()
