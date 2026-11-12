"""Central stage routes. Codex owns providers, credentials and model execution."""
import json
import os
from pathlib import Path
import re
import tomllib

from . import quota_adapter


STAGES = ("analysis", "implementation", "verification")
EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}


def validate(config):
    if not isinstance(config, dict) or type(config.get("version")) is not int or config["version"] != 1:
        raise ValueError("model_routing.version must be 1")
    for stage in STAGES:
        if stage not in config:
            raise ValueError("missing model route: " + stage)
    for stage, route in config.items():
        if stage == "version":
            continue
        if not isinstance(route, dict) or set(route) - {"provider", "model", "profile", "reasoning_effort", "model_provider"}:
            raise ValueError("invalid model route: " + stage)
        if route.get("provider") != "codex":
            raise ValueError("stage provider must be codex: " + stage)
        if not isinstance(route.get("model"), str) or not route["model"].strip():
            raise ValueError("stage model must be an explicit Codex model ID: " + stage)
        if "profile" in route and (not isinstance(route["profile"], str) or not re.fullmatch(r"[A-Za-z0-9_-]+", route["profile"])):
            raise ValueError("invalid Codex profile name: " + stage)
        if "model_provider" in route and (not isinstance(route["model_provider"], str) or not route["model_provider"].strip()):
            raise ValueError("invalid Codex model_provider: " + stage)
        if "reasoning_effort" in route and (not isinstance(route["reasoning_effort"], str) or route["reasoning_effort"] not in EFFORTS):
            raise ValueError("invalid reasoning_effort: " + stage)


def resolve(task, stage):
    """Validate local configuration and the Codex catalog; never probe generation."""
    validate(task.get("model_routing"))
    route = dict(task["model_routing"][stage])
    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    base_path = home / "config.toml"
    base = tomllib.loads(base_path.read_text(encoding="utf-8-sig")) if base_path.exists() else {}
    effective = dict(base)
    if route.get("profile"):
        profile_path = home / (route["profile"] + ".config.toml")
        if not profile_path.is_file():
            raise ValueError("Codex profile file unavailable: " + str(profile_path))
        overlay = tomllib.loads(profile_path.read_text(encoding="utf-8-sig"))
        effective.update(overlay)
        effective["model_providers"] = {**base.get("model_providers", {}), **overlay.get("model_providers", {})}
    provider = route.get("model_provider", effective.get("model_provider", "openai"))
    route["model_provider"] = provider
    if provider == "openai":
        options = ["--profile", route["profile"]] if route.get("profile") else []
        options += ["-c", 'model_provider="openai"']
        catalog = []
        cursor = None
        while True:
            response = quota_adapter.rpc(task, "model/list", {"includeHidden": True, "cursor": cursor}, options)
            catalog.extend(response.get("data", []))
            cursor = response.get("nextCursor")
            if not cursor:
                break
        model = next((m for m in catalog if route["model"] in (m.get("id"), m.get("model"))), None)
        if model is None:
            raise ValueError(f"Codex model unavailable for {stage}: {route['model']}; no fallback")
        efforts = {e.get("reasoningEffort") for e in model.get("supportedReasoningEfforts", [])}
        if route.get("reasoning_effort") and route["reasoning_effort"] not in efforts:
            raise ValueError(f"Codex reasoning effort unavailable for {stage}: {route['reasoning_effort']}")
    else:
        provider_config = effective.get("model_providers", {}).get(provider)
        if not provider_config:
            raise ValueError("Codex provider unavailable: " + provider)
        if route["model"] != effective.get("model"):
            raise ValueError(f"{stage} model does not match the existing Codex profile; no fallback")
        key = provider_config.get("env_key")
        if key and (not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or not os.environ.get(key)):
            raise ValueError(f"Codex provider {provider}: credential environment variable is missing or misconfigured; repair existing Codex configuration")
        if route.get("reasoning_effort") and route["reasoning_effort"] != effective.get("model_reasoning_effort"):
            raise ValueError("custom provider reasoning effort must match its existing Codex profile")
    route["effective_reasoning_effort"] = route.get("reasoning_effort", effective.get("model_reasoning_effort"))
    return route


def arguments(route):
    args = []
    if route.get("profile"):
        args += ["--profile", route["profile"]]
    args += ["--model", route["model"], "-c", "model_provider=" + json.dumps(route["model_provider"])]
    if route.get("reasoning_effort"):
        args += ["-c", "model_reasoning_effort=" + json.dumps(route["reasoning_effort"])]
    return args
