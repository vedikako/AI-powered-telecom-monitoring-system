from __future__ import annotations

from explain.llm import maybe_llm_summary
from explain.rules import explain as rule_explain


def explain_anomaly(metrics: dict, baseline: dict | None = None) -> dict:
    """Rules first. LLM is optional and must never be required for the pipeline."""
    result = rule_explain(metrics, baseline)
    return maybe_llm_summary(result, metrics)
