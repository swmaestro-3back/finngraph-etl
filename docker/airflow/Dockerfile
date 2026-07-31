FROM apache/airflow:3.3.0-python3.11

# Airflow 3.x는 SQLAlchemy>=2.0을 쓰므로 프로젝트(sqlalchemy>=2.0 + psycopg3)를 Airflow와
# 같은 환경에 설치할 수 있다(2.x는 SQLAlchemy<2.0 요구라 불가능했던 구성).
COPY --chown=airflow:root pyproject.toml /opt/etl-src/
COPY --chown=airflow:root pipelines /opt/etl-src/pipelines
RUN pip install --no-cache-dir "/opt/etl-src"

# 마운트 없이 도는 환경(운영)용 기본 사본. 로컬 dev에서는 compose 볼륨이 이 경로를 덮는다.
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root pipelines /opt/airflow/pipelines
COPY --chown=airflow:root scripts /opt/airflow/scripts

# 잡 실행 시 /opt/airflow의 사본(마운트/베이크)이 site-packages 사본보다 우선하게 한다.
ENV PYTHONPATH=/opt/airflow
