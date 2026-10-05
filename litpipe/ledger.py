"""Per-attempt network ledger and the pipeline's one redaction function (dispatch 0.5).

ledger.write(record) appends one JSON line per HTTP attempt to <state_dir>/ledger/<UTC date>.jsonl
(ts, run_id, pid, host, redacted url, method, status, decision, kind, elapsed_ms, bytes,
retry_after, header NAMES, a few safe header values, error, note). Every string in a record passes
through redact() at write time, whatever the caller put there, so no URL, header value or
exception text can carry the email or a key into the file. Appends take a cross-process file lock
(the s2probe pattern) so the runner and an interactive CLI never interleave half-lines.

redact(text) strips, at any percent-encoding depth (%3D, %253D, %25253D, ... as they appear in a
URL nested inside a URL):
  * `email=<value>` and `mailto:<value>` replaced WHOLE, key included, by [EMAIL-REDACTED] /
    [MAILTO-REDACTED], so no persisted string ever contains `email=` or `mailto:` (W4-B's canary is a
    plain grep for them);
  * `api_key`, `apikey`, `api-key`, `access_token` values, as query params or JSON/dict keys;
  * `x-api-key`, `Authorization`, `Proxy-Authorization` values in header or dict text;
  * the configured email (LITPIPE_EMAIL, and lit_util.DEFAULT_EMAIL while it exists) in plain,
    quoted, doubly and trebly quoted, `+`-as-space forms, case-insensitively;
  * any other email address, local part included (a changed LITPIPE_EMAIL mid-run, an echoed
    address in a Location header).
Everything that persists a string from an exception or a URL passes it through redact
(`litpipe.outcomes.legacy_outcome` details included).

Tests redirect the ledger with LEDGER_DIR (tests/conftest.py does it for every test) or by pointing
config.CONFIG_PATH at a temp projects.json with a temp state_dir.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, quote_plus

from litpipe import config

LEDGER_DIR: Path | None = None   # override (tests); default <state_dir>/ledger
RUN_ID: str | None = None        # set by the runner (set_run_id); else LITPIPE_RUN_ID from the parent
PLACEHOLDER = "REDACTED"
SENSITIVE_HEADERS = frozenset({"authorization", "proxy-authorization", "x-api-key", "cookie", "set-cookie"})
LOCK_WAIT_S = 5.0

_HEADER_RE = re.compile(
    r"(?i)\b(x-api-key|proxy-authorization|authorization)(\s*[\"']?\s*[:=]\s*[\"']?\s*)([^\"'\r\n,}]+)")
# %XX at any encoding depth: %3D, %253D, %25253D ... (a URL nested in a URL nested in a URL).
_ENC = r"%(?:25)*"
_PARAM_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9_])((?:api_key|apikey|api-key|access_token)"
    r"(?:[\"']?\s*[:=]\s*[\"']?|" + _ENC + r"3D))"
    r"((?:(?!" + _ENC + r"26)[^&#\s\"'<>,;)])*)")
# Email-bearing tokens are replaced WHOLE, key included, so no persisted string ever contains
# `email=` or `mailto:`: any that appears in a report is an unredacted leak, which keeps the
# canary a plain grep (W4-B). Keys keep `api_key=REDACTED` (no address in them). No lookbehind:
# `contact_email=` and `%26email%3D` must lose their `email=` too.
_EMAIL_PARAM_RE = re.compile(
    r"(?i)e-?mail(?:=|" + _ENC + r"3D)(?:(?!" + _ENC + r"26)[^&#\s\"'<>,;)])*")
EMAIL_TOKEN = "[EMAIL-REDACTED]"
MAILTO_TOKEN = "[MAILTO-REDACTED]"
_MAILTO_RE = re.compile(
    r"(?i)(mailto(?::|" + _ENC + r"3A)\s*)((?:(?!" + _ENC + r"26)[^\s)>\"'&,;])+)")
# Signed, expiring download links (the OSF blob hop to storage.googleapis.com; S3 presigned
# URLs): the signature and credential values work as a bearer token until they expire. No
# lookbehind, so `%26Signature%3D` (a link nested in a link) loses its value too.
_SIGNED_RE = re.compile(
    r"(?i)((?:x-goog-signature|x-goog-credential|x-amz-signature|x-amz-credential|x-amz-security-token"
    r"|awsaccesskeyid|signature)(?:=|" + _ENC + r"3D))"
    r"((?:(?!" + _ENC + r"26)[^&#\s\"'<>,;)])*)")
_ADDRESS_RE = re.compile(
    r"(?i)[A-Za-z0-9._+-]+(?:" + _ENC + r"2B[A-Za-z0-9._+-]*)*(?:@|" + _ENC + r"40)"
    r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]+")

_warned = False


def set_run_id(run_id):
    """The runner calls this once per run (and exports LITPIPE_RUN_ID to its subprocesses)."""
    global RUN_ID
    RUN_ID = run_id


def current_run_id():
    """set_run_id's value, else the run this process registered in litpipe.state, else the
    parent's LITPIPE_RUN_ID; so ledger lines and net's per-run 403 counter share the state run."""
    if RUN_ID:
        return RUN_ID
    try:
        from litpipe import state   # lazy: state imports ledger for redaction
    except ImportError:             # `from pkg import missing` raises plain ImportError (probed)
        return os.environ.get("LITPIPE_RUN_ID") or None
    return state.current_run() or None


def _configured_emails():
    out = []
    env = (os.environ.get("LITPIPE_EMAIL") or "").strip()
    if env:
        out.append(env)
    try:
        import lit_util
        d = getattr(lit_util, "DEFAULT_EMAIL", None)
        if d and d not in out:
            out.append(d)
    except ImportError:
        pass
    return out


def _literal_forms(email):
    forms = {email, email.replace("+", " "), quote_plus(email)}   # '+' form-decoded to a space
    cur = email
    for _ in range(4):                                             # %40, %2540, %252540, ...
        cur = quote(cur, safe="")
        forms |= {cur, quote_plus(cur)}
    for enc in ("%40", "%2540", "%252540"):
        forms.add(email.replace("@", enc))
    return sorted(forms, key=len, reverse=True)


def redact(text):
    """`text` with keys and Authorization values replaced by REDACTED, `email=<value>` and
    `mailto:<value>` replaced whole by EMAIL_TOKEN / MAILTO_TOKEN, and any configured or bare
    address (plain, %40, %2540) by REDACTED. None stays None; anything else is str()-ed first.
    The one redaction implementation for everything the pipeline persists (dispatch 0.5)."""
    if text is None:
        return None
    s = text if isinstance(text, str) else str(text)
    s = _HEADER_RE.sub(lambda m: m.group(1) + m.group(2) + PLACEHOLDER, s)
    s = _PARAM_RE.sub(lambda m: m.group(1) + PLACEHOLDER, s)
    s = _SIGNED_RE.sub(lambda m: m.group(1) + PLACEHOLDER, s)
    s = _EMAIL_PARAM_RE.sub(EMAIL_TOKEN, s)
    s = _MAILTO_RE.sub(MAILTO_TOKEN, s)
    for name in ("S2_API_KEY", "OPENALEX_API_KEY"):        # preflight.KEY_ENVS
        secret = (os.environ.get(name) or "").strip()
        if len(secret) >= 8:
            s = s.replace(secret, PLACEHOLDER)
    for email in _configured_emails():
        for form in _literal_forms(email):
            s = re.sub(re.escape(form), PLACEHOLDER, s, flags=re.IGNORECASE)
    return _ADDRESS_RE.sub(PLACEHOLDER, s)


def redact_headers(headers) -> dict:
    """A plain dict of header values safe to persist: sensitive names are REDACTED outright,
    every other value passes through redact()."""
    out = {}
    for k, v in (headers.items() if hasattr(headers, "items") else headers or ()):
        out[str(k)] = PLACEHOLDER if str(k).lower() in SENSITIVE_HEADERS else redact(v)
    return out


def _deep_redact(x):
    if isinstance(x, str):
        return redact(x)
    if isinstance(x, dict):
        return {k: _deep_redact(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_deep_redact(v) for v in x]
    return x


def ledger_dir(cfg=None) -> Path:
    d = Path(LEDGER_DIR) if LEDGER_DIR is not None else config.state_dir(cfg) / "ledger"
    d.mkdir(parents=True, exist_ok=True)
    return d


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


@contextmanager
def _file_lock(path):
    """Exclusive inter-process lock on a sidecar file (s2probe._FileLock). If the lock cannot be
    had within LOCK_WAIT_S the append goes ahead unlocked rather than losing the line."""
    fh = open(path, "a+b")
    locked = False
    deadline = time.monotonic() + LOCK_WAIT_S
    try:
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except OSError:
                if time.monotonic() > deadline:
                    break
                time.sleep(0.01)
        yield
    finally:
        try:
            if locked:
                if os.name == "nt":
                    import msvcrt
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


def write(record: dict, cfg=None, strict=False) -> Path | None:
    """Append one redacted record. Returns the ledger file, or None when the write failed (a
    warning goes to stderr once; the request it describes is not failed for it) unless strict."""
    global _warned
    rec = _deep_redact(dict(record))
    rec.setdefault("ts", now_iso())
    rec.setdefault("run_id", current_run_id())
    rec.setdefault("pid", os.getpid())
    try:
        d = ledger_dir(cfg)
        path = d / f"{rec['ts'][:10]}.jsonl"
        line = json.dumps(rec, ensure_ascii=False, default=str) + "\n"
        with _file_lock(d / ".ledger.lock"):
            with open(path, "a", encoding="utf-8", newline="\n") as f:
                f.write(line)
        return path
    except OSError as e:
        if strict:
            raise
        if not _warned:
            print(f"[litpipe.ledger] could not write the ledger: {redact(e)}", file=sys.stderr)
            _warned = True
        return None


def read(date=None, cfg=None) -> list[dict]:
    """Records of one UTC date ("YYYY-MM-DD"; default today), or of every file with date="*"."""
    d = ledger_dir(cfg)
    files = sorted(d.glob("*.jsonl")) if date == "*" else [d / f"{date or now_iso()[:10]}.jsonl"]
    out = []
    for p in files:
        if p.exists():
            with open(p, encoding="utf-8") as f:
                out.extend(json.loads(line) for line in f if line.strip())
    return out
