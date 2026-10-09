"""A text sidecar that holds only an abstract is not a text-only holding (VAP, 2026-10-09).

An AHA statistics report deposited in PMC as an author manuscript gave a sidecar of 2,379
characters: the abstract and a pointer to the supplementary material. The PMC stage reported it
sidecar_status OK, so sweep classed the row TEXT_ONLY, as if the article's text were held, and no
worklist received it. litpipe.text.is_abstract_only now measures the body. The PMC stage (new and
existing sidecars) and the preprint stage report such a sidecar as ABSTRACT_ONLY, which sweep routes
like any unfetched row."""
import json
import re

import sweep
from litpipe import text as T
from tests.test_pmc_stage import AM_DOI, AM_PMC, md5, run_stage, world  # noqa: F401  (world is a fixture)
from tests.test_preprint_stage import JSON, env, epmc_record, fx, ris_calls, route_epmc, web  # noqa: F401

MARTIN = {  # the shape of the reported sidecar (2025_Martin_2025HeartDiseaseStrokeStatisticsReport)
    "title": "2025 Heart Disease and Stroke Statistics: A Report of US and Global Data From the American Heart Association",
    "abstract": "The American Heart Association, in conjunction with the National Institutes of Health, "
                "annually reports the most up-to-date statistics related to heart disease. " * 14,
    "sections": [{"title": "Supplementary Material",
                  "text": "Supplemental Material is available at https://www.ahajournals.org/journal/doi/suppl"}],
}
MARTIN["text"] = (f"# {MARTIN['title']}\n\n## Abstract\n\n{MARTIN['abstract']}\n\n## Supplementary Material\n\n"
                  f"{MARTIN['sections'][0]['text']}")


# ---------------------------------------------------------------- the measure
def test_the_reported_sidecar_is_abstract_only_and_a_real_text_is_not():
    assert T.body_chars(MARTIN) < 100 and T.is_abstract_only(MARTIN)
    body = "The cohort was followed for ten years and outcomes were adjudicated blind. " * 30
    full = dict(MARTIN, sections=[{"title": "Methods", "text": body}] + MARTIN["sections"],
                text=MARTIN["text"] + "\n\n## Methods\n\n" + body)
    assert T.body_chars(full) >= T.TEXT_ONLY_MIN_BODY_CHARS and not T.is_abstract_only(full)


def test_an_unstructured_full_text_is_measured_whole_and_odd_records_are_empty():
    long_text = "Paragraph of article text without section structure. " * 60
    assert not T.is_abstract_only({"text": long_text})              # no abstract field: never undercounted
    assert T.body_chars({"text": long_text, "sections": None}) == len(long_text)
    assert T.body_chars(None) == 0 and T.body_chars([]) == 0
    assert not T.is_abstract_only({}) and not T.is_abstract_only({"text": "a few words"})  # says nothing of its parts
    refs = {"text": "x" * 3000, "sections": [{"title": "References", "text": "y" * 2900}]}
    assert T.is_abstract_only(refs)                                  # a reference list is not body
    caption = "Average force in concentric and eccentric phases during one set of contractions. " * 40
    only_floats = {"title": "Comment on: a training concept", "sections": [],   # the shape of a getpaid sidecar:
                   "text": "# Comment on: a training concept\n\n[Fig. 1] " + caption +     # its JATS body has no <sec>
                           "\n[Table 1.] Forces\nset | 1 | 2 | 3\n"}           # so only a caption was kept
    assert len(only_floats["text"]) > 3000 and T.is_abstract_only(only_floats)


# ---------------------------------------------------------------- the PMC stage
def _abstract_only_am(world):
    xml = world.xml[AM_PMC].decode("utf-8")
    assert xml.count("<body>") == 1
    xml = re.sub(r"<body>.*</body>", '<body><sec sec-type="supplementary-material"><title>Supplementary Material'
                 '</title><p>Supplemental Material is available at the publisher.</p></sec></body>', xml, flags=re.S)
    world.xml[AM_PMC] = xml.encode("utf-8")
    meta = world.meta["PMC7983342.1"]
    meta["xml_url"] = f"s3://pmc-oa-opendata/PMC7983342.1/PMC7983342.1.xml?md5={md5(world.xml[AM_PMC])}"


def test_an_abstract_only_author_manuscript_is_not_text_only(world, tmp_path):
    _abstract_only_am(world)
    res, rows, lib = run_stage(tmp_path, [AM_DOI])
    am = rows[AM_DOI]
    assert am["downloaded"] == "False" and am["sidecar"] == "True" and am["sidecar_status"] == "ABSTRACT_ONLY"
    sc = json.loads((lib / (am["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert sc["abstract"] and T.is_abstract_only(sc)                 # the abstract is kept on disk
    assert res["abstract_only"] == 1 and res["text_only"] == 0
    v = sweep.pmc_verdict(am)
    assert not v.text_only
    assert sweep.classify([v])[0] == "TERMINAL_CLOSED"               # routed like any unfetched row


def test_an_existing_abstract_only_sidecar_is_not_text_only_either(world, tmp_path):
    res, rows, lib = run_stage(tmp_path, [AM_DOI])                   # the real AM: a body, TEXT_ONLY
    assert rows[AM_DOI]["sidecar_status"] == "OK" and sweep.pmc_verdict(rows[AM_DOI]).text_only
    path = lib / (rows[AM_DOI]["filename"][:-4] + ".fulltext.json")
    path.write_text(json.dumps(MARTIN), encoding="utf-8")            # an older, abstract-only sidecar
    _, rows, _ = run_stage(tmp_path, [AM_DOI])
    assert rows[AM_DOI]["sidecar_status"] == "ABSTRACT_ONLY" and not sweep.pmc_verdict(rows[AM_DOI]).text_only
    assert json.loads(path.read_text(encoding="utf-8")) == MARTIN      # an existing sidecar is never rewritten


# ---------------------------------------------------------------- the preprint stage (the sibling)
def test_an_abstract_only_preprint_text_is_not_text_only(env):
    env.sources("biorxiv")
    rec = epmc_record("epmc_search_doi_biorxiv_oa.json")
    doi = rec["doi"]
    route_epmc(env.web, by_doi={doi: [rec]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{doi}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    jats = (f'<article><front><article-meta><article-id pub-id-type="doi">{doi}</article-id><title-group>'
            f'<article-title>{rec["title"]}</article-title></title-group><abstract><p>Fibre types adapt.</p>'
            '</abstract></article-meta></front><body><sec><title>Supplementary Material</title><p>See the '
            'supplement.</p></sec></body></article>')
    env.web.add(f"https://www.ebi.ac.uk/europepmc/webservices/rest/{rec['id']}/fullTextXML",
                (200, {"Content-Type": "application/xml"}, jats))
    _, out = env.run([{"doi": doi, "title": rec["title"], "year": "2025", "authors": "Dilbaz S"}])
    r = out[0]
    assert r["sidecar"] == "True" and r["sidecar_status"] == "ABSTRACT_ONLY" and r["outcome"] == "NOT_AVAILABLE"
    assert "abstract only" in r["detail"]
    assert not sweep.preprint_verdict(r).text_only
