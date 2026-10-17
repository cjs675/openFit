"""Credential and configuration resolution.

Precedence mirrors zepp-health-cli: explicit --config, then $ZEPP_CONFIG,
then ./config.json, then ~/.config/zepplocal/config.json, then env vars
(``ZEPP_APP_TOKEN`` etc.) which override file values.

The token is a live bearer credential. It is never logged, never written to
the raw-payload store, and config files are created mode 600.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_HOSTS = {
    "EU": "api-mifit-de2.zepp.com",
    "US": "api-mifit-us2.zepp.com",
    "GLOBAL": "api-mifit.zepp.com",
}

ENV_TOKEN = "ZEPP_APP_TOKEN"
ENV_USER_ID = "ZEPP_USER_ID"
ENV_HOST = "ZEPP_HOST"


class ConfigError(RuntimeError):
    """Raised when required credentials are missing or unusable."""


@dataclass(frozen=True)
class Config:
    app_token: str
    user_id: str
    host: str
    db_path: Path
    timeout: float = 30.0

    @property
    def base_url(self) -> str:
        return f"https://{self.host}"

    def masked_token(self) -> str:
        if len(self.app_token) <= 8:
            return "*" * len(self.app_token)
        return f"{self.app_token[:4]}...{self.app_token[-4:]}"


def config_search_paths(explicit: str | Path | None = None) -> list[Path]:
    paths: list[Path] = []
    if explicit:
        paths.append(Path(explicit))
    env_path = os.environ.get("ZEPP_CONFIG")
    if env_path:
        paths.append(Path(env_path))
    paths.append(Path.cwd() / "config.json")
    paths.append(Path.home() / ".config" / "zepplocal" / "config.json")
    return paths


def load_config(
    db_path: str | Path | None = None,
    *,
    config_path: str | Path | None = None,
    require_credentials: bool = True,
) -> Config:
    """Resolve configuration from files and environment.

    ``require_credentials=False`` suits purely local commands (``status``,
    ``export``, ``schema``), which only need the database path and must keep
    working after a token has expired or before one has been obtained.
    """
    data: dict = {}
    for candidate in config_search_paths(config_path):
        if candidate.is_file():
            try:
                data = json.loads(candidate.read_text())
            except json.JSONDecodeError as exc:
                raise ConfigError(f"{candidate} is not valid JSON: {exc}") from exc
            break

    token = os.environ.get(ENV_TOKEN) or data.get("app_token")
    user_id = os.environ.get(ENV_USER_ID) or data.get("user_id")
    host = os.environ.get(ENV_HOST) or data.get("host")

    resolved_db = Path(
        db_path
        or os.environ.get("ZEPP_DB")
        or data.get("db_path")
        or "data/zepplocal.db"
    )

    if not require_credentials:
        return Config(
            app_token=str(token or ""),
            user_id=str(user_id or ""),
            host=str(host or ""),
            db_path=resolved_db,
            timeout=float(data.get("timeout", 30.0)),
        )

    missing = [
        name
        for name, value in (
            ("app_token", token),
            ("user_id", user_id),
        )
        if not value
    ]
    if missing:
        raise ConfigError(
            f"missing required config: {', '.join(missing)}. "
            "Run `zepplocal init` or set ZEPP_APP_TOKEN / ZEPP_USER_ID."
        )

    if not host:
        raise ConfigError(
            "missing required config: host. It must match your account's region "
            f"({', '.join(sorted(DEFAULT_HOSTS))}) or API calls will return empty."
        )

    return Config(
        app_token=str(token),
        user_id=str(user_id),
        host=str(host),
        db_path=resolved_db,
        timeout=float(data.get("timeout", 30.0)),
    )


def write_config(
    path: str | Path,
    *,
    app_token: str,
    user_id: str,
    host: str,
    db_path: str | Path | None = None,
) -> Path:
    """Write a config file with owner-only permissions."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "app_token": app_token,
        "user_id": str(user_id),
        "host": host,
    }
    if db_path:
        payload["db_path"] = str(db_path)

    path.write_text(json.dumps(payload, indent=2) + "\n")
    path.chmod(0o600)
    return path
