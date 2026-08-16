from __future__ import annotations

import json
from typing import Any, Protocol
from uuid import uuid4

from common.config import settings
from common.db import Json, get_conn
from common.logging import get_logger

log = get_logger("alerts.sink")


class AlertSink(Protocol):
    name: str

    def create_incident(self, alert: dict[str, Any]) -> dict[str, Any]:
        """Return {sys_id, number, sink}."""


def _payload(alert: dict[str, Any]) -> dict[str, Any]:
    evidence = alert.get("evidence") or []
    if isinstance(evidence, list):
        evidence_txt = "\n".join(f"- {e}" for e in evidence)
    else:
        evidence_txt = str(evidence)
    return {
        "short_description": (
            f"{alert.get('possible_cause', 'Network anomaly')} on {alert.get('cell_id')}"
        ),
        "description": (
            f"Cell: {alert.get('cell_id')}\n"
            f"Site: {alert.get('site_id')}\n"
            f"Severity: {alert.get('severity')}\n"
            f"Cause: {alert.get('possible_cause')}\n"
            f"Anomaly score: {alert.get('anomaly_score')}\n\n"
            f"Evidence:\n{evidence_txt}\n\n"
            f"Recommended action:\n{alert.get('recommended_action')}\n"
        ),
        "urgency": "1" if alert.get("severity") == "CRITICAL" else "2",
        "impact": "1" if alert.get("severity") == "CRITICAL" else "2",
        "category": "Network",
        "subcategory": "Telecom cell",
        "caller_id": "telecom-analytics-platform",
    }


class MockServiceNowSink:
    """Local stand-in so the project runs with zero ServiceNow credentials."""

    name = "mock"

    def create_incident(self, alert: dict[str, Any]) -> dict[str, Any]:
        seq = _next_mock_number()
        sys_id = uuid4().hex
        number = f"INC{seq:07d}"
        body = _payload(alert)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.servicenow_incidents
                        (sys_id, number, alert_id, short_description, payload, sink)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sys_id,
                        number,
                        alert.get("alert_id"),
                        body["short_description"],
                        Json(body),
                        self.name,
                    ),
                )
        log.info("mock_incident", extra={"event": "servicenow", "count": seq})
        return {"sys_id": sys_id, "number": number, "sink": self.name}


class ServiceNowSink:
    name = "servicenow"

    def create_incident(self, alert: dict[str, Any]) -> dict[str, Any]:
        import urllib.error
        import urllib.request
        from base64 import b64encode

        instance = settings.servicenow_instance.replace("https://", "").replace(".service-now.com", "")
        url = f"https://{instance}.service-now.com/api/now/table/incident"
        body = json.dumps(_payload(alert)).encode("utf-8")
        token = b64encode(
            f"{settings.servicenow_user}:{settings.servicenow_password}".encode("utf-8")
        ).decode("ascii")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Basic {token}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8"))["result"]
        except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
            log.warning("servicenow_failed_fallback_mock", extra={"event": "servicenow", "error": str(exc)})
            return MockServiceNowSink().create_incident(alert)
        sys_id = data.get("sys_id")
        number = data.get("number")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.servicenow_incidents
                        (sys_id, number, alert_id, short_description, payload, sink)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sys_id,
                        number,
                        alert.get("alert_id"),
                        data.get("short_description"),
                        Json(data),
                        self.name,
                    ),
                )
        return {"sys_id": sys_id, "number": number, "sink": self.name}


def _next_mock_number() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ops.servicenow_incidents")
            return int(cur.fetchone()[0]) + 1


def get_sink() -> AlertSink:
    if settings.servicenow_instance and settings.servicenow_user and settings.servicenow_password:
        return ServiceNowSink()
    return MockServiceNowSink()
