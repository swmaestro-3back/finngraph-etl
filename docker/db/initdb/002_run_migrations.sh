#!/bin/bash
# 이 이미지의 entrypoint는 initdb.d의 .sh를 source가 아니라 exec으로 실행한다.
# shebang과 실행 권한(755)이 둘 다 있어야 한다.
set -e

for migration in /migrations/versions/*.sql; do
  if [ -f "$migration" ]; then
    echo "running migration $migration"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$migration"
  fi
done
