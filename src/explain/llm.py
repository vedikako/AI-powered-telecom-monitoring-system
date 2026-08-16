from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from common.config import settings
from common.logging import get_logger

log = get_logger("explain.llm")


def maybe_llm_summary(rule_result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    """Optional wrapper. If disabled or the call fails, the rule result is returned unchanged."""
    out = dict(rule_result)
    if not settings.llm_enabled or not settings.openai_api_key:
        return out
    prompt = (
        "You are assisting a telecom NOC. In 2 short sentences, restate the likely cause "
        "using only this evidence. Do not invent metrics.\n"
        f"Cause: {rule_result.get('possible_cause')}\n"
        f"Evidence: {rule_result.get('evidence')}\n"
        f"Metrics: {json.dumps(metrics, default=str)[:800]}"
    )
    body = json.dumps(
        {
            "model": settings.llm_model,
            "messages": [
                {"role": "system", "content": "Be concise. Do not invent numbers."},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 120,
            "temperature": 0.2,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.openai_api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        text = payload["choices"][0]["message"]["content"].strip()
        out["llm_summary"] = text
        out["source"] = "rules+llm"
    except (urllib.error.URLError, KeyError, IndexError, TimeoutError, json.JSONDecodeError) as exc:
        log.warning("llm_skipped", extra={"event": "llm", "error": str(exc)})
    return out
