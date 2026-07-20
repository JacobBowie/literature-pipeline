"""Batch 4 guardrail (pin BEFORE c7 touches the fetchers).

migrate_closed_to_md.read_report_chain decides which DOIs get routed to the manual/ILL
.md queue by reading specific columns + magic status strings from the unpaywall / pmc /
preprint report CSVs. c7 rewrites the fetchers' download loop; if it renames a column or
changes a status string, migrate would SILENTLY mis-route a fetched paper to ILL (both
sides use .get() / extrasaction='ignore', so nothing crashes). This locks the contract:
the columns `downloaded`, `oa_status`, `winning_source`, `status`, `doi` and the magic
values `SKIP_EXISTS` / `ALREADY_EXISTS` must survive the consolidation.
"""
import csv
import migrate_closed_to_md as mig


def _write(path, fields, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)


def test_report_chain_routing_contract(tmp_path):
    date = "2026-07-20"
    _write(tmp_path / f"lit_pull_queue.{date}.unpaywall.csv",
           ["doi", "downloaded", "oa_status", "title", "year", "winning_host", "error"], [
               {"doi": "10.1/A", "downloaded": "True",  "oa_status": "open",       "title": "A", "year": "2024", "winning_host": "h", "error": ""},
               {"doi": "10.1/B", "downloaded": "False", "oa_status": "SKIP_EXISTS", "title": "B", "year": "2024", "winning_host": "",  "error": ""},
               {"doi": "10.1/C", "downloaded": "False", "oa_status": "closed",      "title": "C", "year": "2024", "winning_host": "",  "error": ""},
               {"doi": "10.1/D", "downloaded": "False", "oa_status": "closed",      "title": "D", "year": "2024", "winning_host": "",  "error": ""},
           ])
    _write(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
           ["doi", "downloaded", "skipped", "winning_source", "pmcid"], [
               {"doi": "10.1/C", "downloaded": "False", "skipped": "False", "winning_source": "ALREADY_EXISTS", "pmcid": ""},
               {"doi": "10.1/D", "downloaded": "False", "skipped": "False", "winning_source": "",               "pmcid": ""},
           ])
    _write(tmp_path / f"lit_pull_queue.{date}.preprint.csv",
           ["doi", "downloaded", "skipped", "status"], [
               {"doi": "10.1/D", "downloaded": "False", "skipped": "False", "status": "NO_MATCH"},
           ])

    residual = {r["doi"] for r in mig.read_report_chain(tmp_path, date)}
    # A downloaded; B already in lib (SKIP_EXISTS); C resolved by PMC (ALREADY_EXISTS) -> only D
    assert residual == {"10.1/D"}, residual
