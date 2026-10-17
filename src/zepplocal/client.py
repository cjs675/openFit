"""Thin HTTP client for the reverse-engineered Zepp cloud API.

All data endpoints are GET requests authenticated with an ``apptoken``
header. Two failure modes dominate in practice and are handled explicitly:

* HTTP 429 -- Zepp rate-limits aggressively. We honour Retry-After and add
  exponential backoff with jitter so parallel runs don't synchronise.
* HTTP 401/``0102`` -- the token expired (~30 day TTL). That is terminal for
  the run: retrying cannot fix it, so we raise immediately rather than
  burning the retry budget and hammering the account.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

from .config import Config

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
TOKEN_ERROR_MARKERS = ("0102", "invalid token", "token expired")


class ZeppError(RuntimeError):
    """Base error for API failures."""


class AuthError(ZeppError):
    """Token is missing, expired, or issued for the wrong region host."""


class RateLimited(ZeppError):
    """Zepp refused the request outright; retry budget was exhausted."""


class ZeppClient:
    def __init__(
        self,
        config: Config,
        *,
        session: requests.Session | None = None,
        max_retries: int = 4,
        keep_raw: bool = True,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.keep_raw = keep_raw
        self.session.headers.update(
            {
                "apptoken": config.app_token,
                "Accept": "application/json",
                "User-Agent": "zepplocal/0.1",
            }
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Perform a GET and return decoded JSON, retrying transient errors."""
        url = path if path.startswith("http") else f"{self.config.base_url}{path}"
        delay = 1.0

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.timeout,
                )
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise ZeppError(f"network failure for {url}: {exc}") from exc
                self._sleep(delay, url, attempt, reason=str(exc))
                delay *= 2
                continue

            if response.status_code in RETRYABLE_STATUS:
                if attempt == self.max_retries:
                    raise RateLimited(
                        f"{url} returned {response.status_code} after "
                        f"{self.max_retries} attempts"
                    )
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                wait = retry_after if retry_after is not None else delay
                self._sleep(wait, url, attempt, reason=f"HTTP {response.status_code}")
                delay *= 2
                continue

            if response.status_code in (401, 403):
                raise AuthError(
                    f"{url} returned {response.status_code}. The token is likely "
                    "expired (~30 day TTL) or the host does not match your region."
                )

            if response.status_code == 404:
                # A 404 usually means the path is wrong, not that the data is
                # absent. Surfacing the path makes that easy to spot.
                raise ZeppError(f"{url} returned 404 -- unknown endpoint?")

            if not response.ok:
                raise ZeppError(f"{url} returned {response.status_code}")

            body = self._decode(response, url)
            self._assert_not_token_error(body, url)
            return body

        raise ZeppError(f"exhausted retries for {url}")

    def _decode(self, response: requests.Response, url: str) -> Any:
        if self.keep_raw:
            self._store_raw(url, response)
        try:
            return response.json()
        except ValueError as exc:
            snippet = response.text[:200].replace("\n", " ")
            raise ZeppError(
                f"{url} returned non-JSON content: {snippet!r}"
            ) from exc

    def _store_raw(self, url: str, response: requests.Response) -> None:
        """Persist the raw body so parsers can be rebuilt without re-fetching.

        Failures here are swallowed: losing an audit copy must never abort a
        sync.
        """
        try:
            from .schema import connect

            conn = connect(self.config.db_path)
            conn.execute(
                "INSERT INTO raw_payloads (endpoint, fetched_at, status, body) "
                "VALUES (?, datetime('now'), ?, ?)",
                (url, response.status_code, response.text),
            )
            conn.commit()
            conn.close()
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("raw payload store failed for %s: %s", url, exc)

    @staticmethod
    def _assert_not_token_error(body: Any, url: str) -> None:
        """Detect Zepp's in-body auth failure, which returns HTTP 200."""
        if isinstance(body, dict):
            error = body.get("error")
            code = str(error) if error is not None else ""
            if any(marker in code.lower() for marker in TOKEN_ERROR_MARKERS):
                raise AuthError(f"{url} reported an auth error in the body: {body}")
            result = body.get("result")
            if isinstance(result, str) and any(
                marker in result.lower() for marker in TOKEN_ERROR_MARKERS
            ):
                raise AuthError(f"{url} reported an auth error in the body: {body}")

    def _sleep(
        self, seconds: float, url: str, attempt: int, *, reason: str
    ) -> None:
        # Full jitter: prevents every worker from retrying on the same tick.
        wait = random.uniform(0, seconds) if reason != "retry-after" else seconds
        log.warning(
            "retrying %s (attempt %d/%d) in %.1fs: %s",
            url,
            attempt,
            self.max_retries,
            wait,
            reason,
        )
        time.sleep(wait)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
