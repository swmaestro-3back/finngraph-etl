# Neo4j (local)

테마 파이프라인(`pipelines/themes/`)이 사용하는 그래프 DB의 로컬 개발용 인스턴스입니다.

- 이미지: `dhi.io/neo4j:5-debian-dev` (Docker Hardened Image — zero-CVE, 비루트 UID 65532 실행)
- APOC/GDS 플러그인 불필요 (코어 Cypher만 사용)
- 데이터는 named volume `etl_neo4j_data`에 보존됩니다. 초기화하려면 `docker compose down -v`.

## 기동

```bash
docker compose up -d neo4j
```

- Bolt: `bolt://localhost:7687` (`.env`의 `NEO4J_URI`)
- 브라우저 UI: http://localhost:7474 (`.env`의 `NEO4J_USERNAME`/`NEO4J_PASSWORD`로 로그인)
- 초기 비밀번호는 compose의 `NEO4J_AUTH`가 설정하며, **볼륨 최초 생성 시에만** 적용됩니다.
  비밀번호를 바꾼 뒤 반영이 안 되면 `docker compose down -v`로 볼륨을 초기화하세요.

운영 배포 시에는 `-dev` 태그 대신 명시적 버전의 runtime 태그로 고정하는 것을 권장합니다.
