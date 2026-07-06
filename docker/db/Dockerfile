ARG BASE_IMAGE=timescale/timescaledb:latest-pg16
FROM ${BASE_IMAGE}

ARG PGVECTOR_VERSION=v0.8.4

RUN set -eux; \
    apk add --no-cache --virtual .pgvector-build-deps \
      build-base \
      clang \
      git \
      llvm \
      llvm-dev \
      postgresql-dev; \
    git clone --depth 1 --branch "${PGVECTOR_VERSION}" https://github.com/pgvector/pgvector.git /tmp/pgvector; \
    make -C /tmp/pgvector OPTFLAGS=""; \
    make -C /tmp/pgvector install; \
    rm -rf /tmp/pgvector; \
    apk del .pgvector-build-deps

