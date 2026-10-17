from __future__ import annotations

import logging

log = logging.getLogger(__name__)

SLACK_CONN_ID = "slack_alerts"


def _slack_failure_callback(context) -> None:

    try:
        from airflow.providers.slack.notifications.slack_webhook import (
            send_slack_webhook_notification,
        )

        ti = context["ti"]
        exception = context.get("exception")
        lines = [
            f":rotating_light: *DAG 실패* `{ti.dag_id}`",
            f"• 태스크: `{ti.task_id}` (try {ti.try_number})",
            f"• 논리 시각: {context.get('logical_date')}",
        ]
        log_url = getattr(ti, "log_url", None)
        if log_url:
            lines.append(f"• 로그: {log_url}")
        if exception:
            lines.append(f"• 예외: `{type(exception).__name__}: {exception}`")

        send_slack_webhook_notification(
            slack_webhook_conn_id=SLACK_CONN_ID,
            text="\n".join(lines),
        )(context)
    except Exception:
        log.warning(
            "Slack 실패 알림 전송 실패 (커넥션 %s 미설정이면 정상)",
            SLACK_CONN_ID,
            exc_info=True,
        )


def task_policy(task) -> None:
    existing = task.on_failure_callback
    if existing is None:
        task.on_failure_callback = [_slack_failure_callback]
    elif isinstance(existing, list):
        if _slack_failure_callback not in existing:
            existing.append(_slack_failure_callback)
    else:
        task.on_failure_callback = [existing, _slack_failure_callback]
