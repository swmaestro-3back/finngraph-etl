FROM flyway/flyway:12-alpine

ENV FLYWAY_POSTGRESQL_TRANSACTIONAL_LOCK=false

COPY migrations/versions /flyway/sql

CMD ["migrate"]
