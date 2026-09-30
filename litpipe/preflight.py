"""Pre-flight: what the runner checks before any network stage (dispatch W1-A2 step 2; refactor
scope 2.1; plan DEC-13). `run() -> list[Outcome]`; the run goes ahead only when `ok(outcomes)`.

Checks, in order:
  email    LITPIPE_EMAIL must be set and must not use a domain RFC 2606 reserves (example.com,
           example.net, example.org and their subdomains; the TLDs .test, .example, .invalid,
           .localhost). Anything else is CONFIG with a one-line fix. Unpaywall itself rejects only
           example.com (422 "Please use your own email address"; example.org got 200 live on
           2026-09-30), and Crossref said on 2026-07-21 that repeated invalid addresses can get
           API access blocked, so a placeholder is never sent: the network checks are SKIPPED.
  unpaywall  GET api.unpaywall.org/v2/10.1371/journal.pone.0012033 must answer 200 with that
           DOI's record. litpipe.net adds email= itself (the host's identity mode), so the check
           exercises the pipeline's real identity path. 422 or 410 is CONFIG (bad or missing
           email, retired endpoint).
  crossref GET api.crossref.org/works/10.1371/journal.pone.0012033 with litpipe.net's identity
           (mailto in the User-Agent) must come back in the polite pool: header x-api-pool
           "polite-single" (live 2026-09-30, with x-rate-limit-limit 10, x-rate-limit-interval 1s,
           x-concurrency-limit 3). A public pool is CONFIG; a missing header is ERROR (drift).
  keys     S2_API_KEY and OPENALEX_API_KEY: present or absent. The value is never read into any
           output, detail or payload.
  writer   no other live run holds the writer role (litpipe.state.live_runs; REG-I27). DEFERRED
           when one does: try again later.

Network checks go through litpipe.net.request (imported lazily) so they see the same identity,
pacing, refusals and redaction as every stage; tests pass a fake `request`.
"""
import argparse
import json
import os
import re
import sys

from litpipe.outcomes import Kind, Outcome

PREFLIGHT_DOI = "10.1371/journal.pone.0012033"
UNPAYWALL_HOST = "api.unpaywall.org"
CROSSREF_HOST = "api.crossref.org"
UNPAYWALL_URL = f"https://{UNPAYWALL_HOST}/v2/{PREFLIGHT_DOI}"
CROSSREF_URL = f"https://{CROSSREF_HOST}/works/{PREFLIGHT_DOI}"
EMAIL_ENV = "LITPIPE_EMAIL"
KEY_ENVS = ("S2_API_KEY", "OPENALEX_API_KEY")
RESERVED_DOMAINS = ("example.com", "example.net", "example.org")   # RFC 2606 section 3
RESERVED_TLDS = ("test", "example", "invalid", "localhost")         # RFC 2606 section 2
POLITE_POOLS = ("polite", "plus")                                   # x-api-pool prefixes
EMAIL_FIX = ("set LITPIPE_EMAIL to your own contact address "
             "(PowerShell: setx LITPIPE_EMAIL you@your.edu, then open a new shell)")
PASS_KINDS = frozenset({Kind.OK, Kind.SKIPPED})


def _o(kind, check, detail, host="", status=None, **extra):
    return Outcome(kind, status=status, host=host, detail=detail, payload={"check": check, **extra})


# ------------------------------------------------------------------------------ email and keys
def email_problem(value):
    """Why `value` is not a usable contact address, or None when it is."""
    if value is None:
        return "LITPIPE_EMAIL is unset"
    v = value.strip()
    if not v:
        return "LITPIPE_EMAIL is empty"
    local, at, domain = v.rpartition("@")
    domain = domain.lower().rstrip(".")
    if not at or not local or not domain or re.search(r"\s", v):
        return "LITPIPE_EMAIL is not an email address"
    tld = domain.rsplit(".", 1)[-1]
    for d in RESERVED_DOMAINS:
        if domain == d or domain.endswith("." + d):
            return f"LITPIPE_EMAIL uses the reserved placeholder domain {d} (RFC 2606)"
    if tld in RESERVED_TLDS:
        return f"LITPIPE_EMAIL uses the reserved top-level domain .{tld} (RFC 2606)"
    if "." not in domain:
        return "LITPIPE_EMAIL is not an email address"
    return None


def check_email(env):
    problem = email_problem(env.get(EMAIL_ENV))
    if problem:
        return _o(Kind.CONFIG, "email", f"{problem}; {EMAIL_FIX}", host="env")
    return _o(Kind.OK, "email", "LITPIPE_EMAIL is set (value not shown)", host="env")


def check_keys(env):
    out = []
    for name in KEY_ENVS:
        present = bool((env.get(name) or "").strip())
        out.append(_o(Kind.OK, name.lower(), f"{name} {'present' if present else 'absent'}",
                      host="env", present=present))
    return out


# ------------------------------------------------------------------------------ network checks
def _parts(out):
    """(status, lower-cased headers, body) from a litpipe.net Outcome; the payload carries the
    response (dispatch W1-A1: status, headers, content), as a dict or a response-like object."""
    p, status, headers, body = out.payload, out.status, {}, None
    if isinstance(p, dict):
        status = p.get("status", status)
        headers = p.get("headers") or {}
        for k in ("content", "body", "json", "text"):
            if p.get(k) is not None:
                body = p[k]
                break
    elif p is not None:
        status = getattr(p, "status_code", getattr(p, "status", status))
        headers = getattr(p, "headers", None) or {}
        body = getattr(p, "content", None)
    items = headers.items() if hasattr(headers, "items") else headers
    return status, {str(k).lower(): str(v) for k, v in items}, body


def _json(body):
    if isinstance(body, (dict, list)):
        return body
    try:
        if isinstance(body, (bytes, bytearray)):
            body = body.decode("utf-8")
        return json.loads(body) if isinstance(body, str) else None
    except (ValueError, UnicodeDecodeError):
        return None


def _failed(check, host, out, status):
    kind = out.kind if out.kind not in PASS_KINDS else Kind.ERROR
    return _o(kind, check, f"{host} answered {status or 'nothing'} ({out.kind}): {out.detail}"[:300],
              host=host, status=status)


def check_unpaywall(request):
    # No email here: litpipe.net injects it for this host (and drops a caller's email=), so a
    # broken identity path shows up as a 422 instead of being masked by a hand-added parameter.
    out = request("GET", UNPAYWALL_URL, purpose="preflight")
    status, _, body = _parts(out)
    if status in (410, 422):
        j = _json(body)
        msg = j.get("message", "") if isinstance(j, dict) else ""
        return _o(Kind.CONFIG, "unpaywall", f"Unpaywall answered {status} {msg}; {EMAIL_FIX}"[:300],
                  host=UNPAYWALL_HOST, status=status)
    if status != 200 or out.kind is not Kind.OK:
        return _failed("unpaywall", UNPAYWALL_HOST, out, status)
    j = _json(body)
    if not isinstance(j, dict) or str(j.get("doi") or "").lower() != PREFLIGHT_DOI:
        return _o(Kind.OUTAGE, "unpaywall", "Unpaywall 200 without the preflight DOI's record "
                  "(an HTML page or a changed format)", host=UNPAYWALL_HOST, status=status)
    return _o(Kind.OK, "unpaywall", f"Unpaywall 200 on {PREFLIGHT_DOI}", host=UNPAYWALL_HOST,
              status=status, is_oa=j.get("is_oa"), updated=j.get("updated"))


def check_crossref(request):
    out = request("GET", CROSSREF_URL, purpose="preflight")
    status, headers, _ = _parts(out)
    if status != 200 or out.kind is not Kind.OK:
        return _failed("crossref", CROSSREF_HOST, out, status)
    pool = headers.get("x-api-pool")
    limits = {k: headers.get(k) for k in ("x-rate-limit-limit", "x-rate-limit-interval",
                                          "x-concurrency-limit", "x-rate-limit-type")}
    if pool is None:
        return _o(Kind.ERROR, "crossref", "Crossref sent no x-api-pool header (drift): the pool "
                  "cannot be confirmed", host=CROSSREF_HOST, status=status, **limits)
    if not pool.lower().startswith(POLITE_POOLS):
        return _o(Kind.CONFIG, "crossref", f"Crossref put this client in the {pool} pool, not "
                  f"polite: the mailto identity was not recognised; {EMAIL_FIX}",
                  host=CROSSREF_HOST, status=status, pool=pool, **limits)
    return _o(Kind.OK, "crossref", f"Crossref pool {pool} ({limits['x-rate-limit-limit']} per "
              f"{limits['x-rate-limit-interval']}, concurrency {limits['x-concurrency-limit']})",
              host=CROSSREF_HOST, status=status, pool=pool, **limits)


def _net_request():
    try:
        import litpipe.net as net
    except ModuleNotFoundError as e:
        if e.name != "litpipe.net":
            raise
        return None
    return net.request


def _guarded(fn, check, host, *args):
    """A preflight check never raises: an exception is an ERROR outcome (its text redacted)."""
    try:
        return fn(*args)
    except Exception as e:  # noqa: BLE001 - reported, not swallowed
        from litpipe.state import _redact
        return _o(Kind.ERROR, check, _redact(f"{type(e).__name__}: {e}")[:300], host=host)


# ------------------------------------------------------------------------------ writer role
def check_writers(own_run_id=None):
    from litpipe import state
    mine = {r for r in (own_run_id, state.current_run()) if r}
    others = [r for r in state.live_runs()
              if r["writer"] and r["run_id"] not in mine and r["pid"] != os.getpid()]
    if others:
        r = others[0]
        return _o(Kind.DEFERRED, "writer", f"another run holds the writer role: {r['kind']} "
                  f"{r['run_id']} (pid {r['pid']}, heartbeat {r['heartbeat_age_s']}s ago); "
                  f"try again when it finishes", host="state", runs=[x["run_id"] for x in others])
    return _o(Kind.OK, "writer", "no other live writer run", host="state")


# ------------------------------------------------------------------------------ run
def _secrets(env):
    vals = [env.get(n) for n in (EMAIL_ENV, *KEY_ENVS)]
    forms = set()
    for v in vals:
        v = (v or "").strip()
        if len(v) >= 4:
            forms |= {v, v.replace("@", "%40"), v.replace("@", "%2540")}
    return sorted(forms, key=len, reverse=True)


def _scrub(o, forms):
    """Last guard: no email or key literal leaves preflight, whatever a transport put in a detail."""
    detail = o.detail
    for f in forms:
        detail = re.sub(re.escape(f), "<REDACTED>", detail, flags=re.IGNORECASE)
    if detail == o.detail:
        return o
    return Outcome(o.kind, o.status, o.host, detail, o.attempts, o.elapsed_ms, o.retry_after,
                   o.payload)


def run(*, env=None, request=None, own_run_id=None, network=True) -> list[Outcome]:
    """Run every check; see the module docstring. `request` replaces litpipe.net.request."""
    env = os.environ if env is None else env
    outs = [check_email(env)]
    if not network:
        outs += [_o(Kind.SKIPPED, c, "network checks off (--no-network)", host=h)
                 for c, h in (("unpaywall", UNPAYWALL_HOST), ("crossref", CROSSREF_HOST))]
    elif not outs[0].ok:
        outs += [_o(Kind.SKIPPED, c, "not sent: no usable LITPIPE_EMAIL", host=h)
                 for c, h in (("unpaywall", UNPAYWALL_HOST), ("crossref", CROSSREF_HOST))]
    else:
        req = request or _net_request()
        if req is None:
            outs += [_o(Kind.ERROR, c, "litpipe.net is not available, so this check cannot run",
                        host=h) for c, h in (("unpaywall", UNPAYWALL_HOST),
                                             ("crossref", CROSSREF_HOST))]
        else:
            outs.append(_guarded(check_unpaywall, "unpaywall", UNPAYWALL_HOST, req))
            outs.append(_guarded(check_crossref, "crossref", CROSSREF_HOST, req))
    outs += check_keys(env)
    outs.append(_guarded(check_writers, "writer", "state", own_run_id))
    forms = _secrets(env)
    return [_scrub(o, forms) for o in outs]


def ok(outcomes) -> bool:
    """True when the run may go ahead: every check OK or deliberately SKIPPED."""
    return all(o.kind in PASS_KINDS for o in outcomes)


def exit_code(outcomes) -> int:
    """0 go; 2 a CONFIG problem (fix the setup); 1 anything else (try later or investigate)."""
    if ok(outcomes):
        return 0
    return 2 if any(o.kind is Kind.CONFIG for o in outcomes) else 1


def main(argv=None, *, env=None, request=None):
    ap = argparse.ArgumentParser(
        prog="python -m litpipe.preflight",
        description="Pre-flight checks before a pipeline run: contact email, Unpaywall, the "
                    "Crossref pool, API keys (present or absent only) and other live writer runs. "
                    "Exit 0 go, 2 configuration problem, 1 anything else.")
    ap.add_argument("--no-network", action="store_true",
                    help="skip the Unpaywall and Crossref requests")
    ap.add_argument("--json", action="store_true", help="print the outcomes as JSON")
    args = ap.parse_args(argv)
    outs = run(env=env, request=request, network=not args.no_network)
    if args.json:
        print(json.dumps([{"check": (o.payload or {}).get("check"), "kind": str(o.kind),
                           "status": o.status, "host": o.host, "detail": o.detail}
                          for o in outs], indent=2))
    else:
        for o in outs:
            print(f"{o.kind:<9} {(o.payload or {}).get('check', ''):<16} {o.detail}")
        print("preflight: " + ("go" if ok(outs) else "STOP"))
    return exit_code(outs)


if __name__ == "__main__":
    sys.exit(main())
