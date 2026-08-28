#!/usr/bin/env bash
# Parameter Store 의 값으로 .env.dev 를 만든다. 기본값을 두지 않아 누락은 기동 실패로 드러난다.
set -euo pipefail

REGION=${AWS_REGION:-ap-northeast-2}
PREFIX=${PARAM_PREFIX:-/finngraph/dev}
OUT=${1:-.env.dev}

command -v aws >/dev/null || { echo "aws CLI 가 없습니다" >&2; exit 1; }

param() {
  local v
  v=$(aws ssm get-parameter --region "$REGION" --name "$1" --with-decryption \
      --query Parameter.Value --output text 2>/dev/null) || true
  if [ -z "$v" ] || [ "$v" = "None" ]; then
    echo "파라미터 없음: $1" >&2; exit 1
  fi
  printf '%s' "$v"
}

tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT

# ── Parameter Store (경로 마지막 마디가 변수 이름) ─────────────────────────
aws ssm get-parameters-by-path --region "$REGION" --path "$PREFIX/env" --recursive \
    --with-decryption --query 'Parameters[].[Name,Value]' --output text \
  | while IFS=$'\t' read -r name value; do
      printf '%s=%s\n' "${name##*/}" "$value"
    done > "$tmp"

# ── 접속 문자열 ────────────────────────────────────────────────────────────
# 비밀번호를 통째로 저장하지 않고 조립한다. 저장하면 교체 시 여러 곳을 고쳐야 한다.
RDS_HOST=$(grep -m1 '^DB_HOST=' "$tmp" | cut -d= -f2-)
[ -n "$RDS_HOST" ] || { echo "DB_HOST 파라미터가 없습니다" >&2; exit 1; }

ETL_PW=$(param "$PREFIX/etl/db_password")
AF_PW=$(param "$PREFIX/airflow/db_password")
NEO_PW=$(param "$PREFIX/neo4j/password")

# rds.force_ssl=1 이라 sslmode=require 가 없으면 접속이 거부된다.
cat >> "$tmp" <<EOF
DB_PORT=5432
DB_NAME=finngraph_dev
DB_USER=etl_app
DB_PASSWORD=$ETL_PW
DATABASE_URL=postgresql+psycopg://etl_app:$ETL_PW@$RDS_HOST:5432/finngraph_dev?sslmode=require
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_app:$AF_PW@$RDS_HOST:5432/airflow?sslmode=require
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=$NEO_PW
NEO4J_DATABASE=finngraph
EOF

# ── 검증 ────────────────────────────────────────────────────────────────────
missing=()
for k in DATABASE_URL AIRFLOW__DATABASE__SQL_ALCHEMY_CONN NEO4J_URI NEO4J_PASSWORD \
         AIRFLOW_ADMIN_PASSWORD AIRFLOW_FERNET_KEY KIS_APP_KEY DART_API_KEY; do
  grep -q "^${k}=" "$tmp" || missing+=("$k")
done
[ ${#missing[@]} -eq 0 ] || { printf '없는 필수 키: %s\n' "${missing[*]}" >&2; exit 1; }

install -m 600 "$tmp" "$OUT"
echo "$OUT 생성 완료 ($(grep -c . "$OUT")개, 권한 600)"
