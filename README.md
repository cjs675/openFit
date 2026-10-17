# zepplocal

Local-first sync for **AmazFit Helio Strap** data. Pulls your sleep, HR, HRV,
SpO₂, stress, skin temperature, workouts and BioCharge/recovery metrics out of
the Zepp cloud and into a SQLite database you own.

No reverse engineering of the strap hardware is required. The Zepp cloud API
already exposes everything the device records.

## Status

Working and tested (74 tests passing). The endpoint paths are reverse-engineered
from community tooling and **not yet verified against a live account** — run
`probe` first (below) and open an issue with any that fail.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Getting a token

Zepp's API has no supported programmatic login. Tokens are obtained from a
browser session:

1. Log in at <https://user.huami.com/privacy2/index.html>
2. Open DevTools (F12) → **Application** → **Cookies**
3. Copy the `apptoken` and `userid` values

> Use the **Application → Cookies** tab, not the Network tab. Older
> instructions say Network, but that fails against Zepp's cookie-based auth.

### Which host

The region host must match your account or every call returns empty data.
While you're on that page, note the `api-mifit*.zepp.com` host used by any
request. Known values:

| Region | Host |
| --- | --- |
| Europe | `api-mifit-de2.zepp.com` |
| Americas | `api-mifit-us2.zepp.com` |
| Global | `api-mifit.zepp.com` |

Tokens expire after ~30 days. Re-run `init` with a fresh one when sync starts
failing with an auth error.

## Configure

```bash
zepplocal init --token <apptoken> --user-id <userid> --host api-mifit-us2.zepp.com
```

Writes `config.json` with mode `600`. Credentials can also come from the
environment (`ZEPP_APP_TOKEN`, `ZEPP_USER_ID`, `ZEPP_HOST`), which override
file values — useful for CI.

## Use

```bash
# Verify every endpoint without writing data
zepplocal probe

# Fetch the last 30 days
zepplocal sync --days 30

# Just one stream
zepplocal sync --days 7 --stream sleep --stream hrv

# Row counts, coverage, per-stream cursor and error state
zepplocal status

# Export
zepplocal export daily --out daily.csv
zepplocal export sleep --from-date 2025-06-01
```

A single failing stream never aborts the run; each records its own status in
`sync_state` so `status` shows exactly what succeeded.

## Schema

`zepplocal schema` prints the full SQL — that's the reference for porting to
iOS.

Design decisions worth knowing before you change anything:

- **Raw samples are never aggregated at write time.** Daily figures are
  `VIEW`s, so you can recompute them without re-importing history.
- **Every table has a natural primary key and all writes are UPSERTs.**
  Re-running any window is safe.
- **All timestamps are UTC ISO-8601.** `date_local` is stored alongside so
  day boundaries survive timezone changes. Set `to_local_date(..., offset)`
  if you want local days computed rather than assumed UTC.
- **Per-second HR is isolated** in `hr_samples_per_second` (86,400 rows/day)
  so it can be pruned without affecting anything else.
- **Skin temperature is a delta**, not an absolute: Zepp reports hundredths of
  a degree C above your personal baseline (`26` = +0.26 °C).

## Architecture

```
config.py    credential resolution, mode-600 config files
client.py    HTTP + retry/backoff/rate-limit, in-body auth-error detection
parsers.py   defensive normalisation of unstable Zepp payloads
streams.py   endpoint → parser → table mappings
sync.py      resumable backfill, per-stream cursors, idempotent upserts
schema.py    SQLite DDL + connection helper
cli.py       command surface
```

`parsers.py` never raises on an unknown shape — it imports what it recognises
and leaves the rest `None`. Raw response bodies are archived in
`raw_payloads`, so when Zepp changes a payload you can re-parse old data
without re-fetching it.

## Porting to iOS

The schema and sync logic are deliberately platform-neutral. For a native app:

1. Reuse the table/view definitions verbatim (`zepplocal schema`).
2. Port the parsers — they depend only on `json` and `datetime`.
3. Token entry becomes a paste-a-token field. The browser-cookie trick is a
   Mac workflow and can't run on-device, so the app needs a token pasted from
   desktop, or an email/password login flow that Zepp breaks periodically.
4. Background refresh is the real constraint: iOS `BGAppRefreshTask` is
   best-effort and gets far less time than a Mac daemon, so design for
   catch-up syncs on app launch rather than a strict schedule.

## Legal

Uses Zepp's undocumented cloud API. Not affiliated with Zepp Health / Huami.
Calling private endpoints may violate their terms in some jurisdictions. You
are reading your own data; you are responsible for complying.

## References

- [zepp-health-cli](https://github.com/m4ary/zepp-health-cli) — endpoint catalogue
- [zepp-bridge](https://github.com/kikienrico/zepp-bridge) — built on a Helio Strap; region hosts
- [pulsebridge](https://github.com/101mlevydev/pulsebridge) — resumable sync pattern
