"""KIS 접근토큰 캐시 유닛 테스트.

토큰 발급에는 분당 1회 제한이 있어 캐시가 틀리면 곧바로 한도에 걸린다. 네트워크를 타지
않도록 저장소를 가짜로 바꿔 끼우고, 캐시를 "쓸 수 있다/없다"로 가르는 판정만 검증한다.
"""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from contextlib import nullcontext
from pathlib import Path

from pipelines.common.clients.kis import CachedToken, FileTokenStore, KisClient

APP_KEY = "appkey-1"


class FakeTokenStore:
    """메모리에만 담아두는 저장소. 락은 없다."""

    def __init__(self, token: CachedToken | None = None) -> None:
        self.token = token

    def read(self) -> CachedToken | None:
        return self.token

    def write(self, token: CachedToken) -> None:
        self.token = token

    def issue_lock(self):
        return nullcontext()


class FakeSettings:
    kis_base_url = "https://example.invalid"
    kis_app_key = APP_KEY
    kis_app_secret = "secret"
    kis_rate_limit_per_second = 15
    kis_token_cache_path = "/tmp/does-not-matter.json"


def _client(token: CachedToken | None) -> KisClient:
    return KisClient(FakeSettings(), FakeTokenStore(token))


class ValidTokenTest(unittest.TestCase):
    def test_live_token_is_reused(self) -> None:
        client = _client(CachedToken("live", time.time() + 86400, APP_KEY))

        self.assertEqual(client._valid_token(), "live")

    def test_empty_store_is_a_miss(self) -> None:
        self.assertIsNone(_client(None)._valid_token())

    def test_token_near_expiry_is_a_miss(self) -> None:
        """만료 여유(10분) 안에 든 토큰은 쓰지 않는다.

        호출 도중 만료돼 401을 맞는 것보다 미리 새로 받는 편이 싸다.
        """

        client = _client(CachedToken("stale", time.time() + 60, APP_KEY))

        self.assertIsNone(client._valid_token())

    def test_token_from_another_app_key_is_a_miss(self) -> None:
        """앱키가 바뀌면(운영↔모의) 남은 토큰을 쓰면 안 된다."""

        client = _client(CachedToken("other", time.time() + 86400, "appkey-2"))

        self.assertIsNone(client._valid_token())


class FileTokenStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.path = Path(self._dir.name) / "token.json"

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_round_trip(self) -> None:
        store = FileTokenStore(self.path)
        expires_at = time.time() + 86400

        store.write(CachedToken("tok", expires_at, APP_KEY))
        loaded = store.read()

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.access_token, "tok")
        self.assertEqual(loaded.app_key, APP_KEY)
        self.assertAlmostEqual(loaded.expires_at, expires_at, places=3)

    def test_missing_file_is_none(self) -> None:
        self.assertIsNone(FileTokenStore(self.path).read())

    def test_corrupt_file_is_none(self) -> None:
        """반쯤 쓰인 파일을 만나도 예외 대신 캐시 미스로 다룬다."""

        self.path.write_text('{"access_token": "tok"', encoding="utf-8")

        self.assertIsNone(FileTokenStore(self.path).read())

    def test_file_without_expiry_is_none(self) -> None:
        self.path.write_text(json.dumps({"access_token": "tok"}), encoding="utf-8")

        self.assertIsNone(FileTokenStore(self.path).read())

    def test_token_file_is_not_world_readable(self) -> None:
        """접근토큰은 자격증명이다. 다른 사용자가 읽을 수 있으면 안 된다."""

        store = FileTokenStore(self.path)
        store.write(CachedToken("tok", time.time() + 86400, APP_KEY))

        self.assertEqual(self.path.stat().st_mode & 0o077, 0)

    def test_write_failure_does_not_raise(self) -> None:
        """캐시는 최적화일 뿐이라 기록에 실패해도 호출은 계속되어야 한다."""

        store = FileTokenStore(Path("/proc/nonexistent-dir/token.json"))

        store.write(CachedToken("tok", time.time() + 86400, APP_KEY))


if __name__ == "__main__":
    unittest.main()
