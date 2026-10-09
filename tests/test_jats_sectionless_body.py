"""Paragraphs placed straight in a JATS <body> are text too (2026-10-09).

parse_jats read only <body>/<sec>, so a comment, a letter or an editorial whose body has no
sections lost every paragraph: a library held a letter's sidecar with its title and one figure
caption, and nothing it said. Paragraphs outside a <sec> now form untitled sections where they
stand."""
import jats_to_text as J
from litpipe import text as T

COMMENT = (b'<article><front><article-meta><title-group><article-title>Comment on: a training concept'
           b'</article-title></title-group><abstract><p>Short abstract.</p></abstract></article-meta></front>'
           b'<body>'
           + b"".join(b"<p>The comment's paragraph " + str(i).encode() + b" argues the load reduction misreads "
                      b"the force data and the device's resistance profile.</p>" for i in range(20))
           + b'<fig id="f1"><label>Fig. 1</label><caption><p>Average force per repetition.</p></caption></fig>'
             b'</body></article>')


def test_a_comment_with_no_sections_keeps_its_paragraphs():
    d = J.parse_jats(COMMENT)
    assert [s["title"] for s in d["sections"]] == [""]
    assert "paragraph 0 argues" in d["sections"][0]["text"] and "paragraph 19 argues" in d["sections"][0]["text"]
    assert "paragraph 7 argues" in d["text"]
    assert [f["label"] for f in d["figures"]] == ["Fig. 1"]                 # the figure is still collected
    assert not T.is_abstract_only(d)                                     # a held text, not an abstract


def test_loose_paragraphs_keep_their_place_around_sections_and_references_stay_out():
    xml = (b'<article><front><article-meta><title-group><article-title>T</article-title></title-group>'
           b'</article-meta></front><body><p>An untitled opening.</p><sec><title>Methods</title><p>M.</p></sec>'
           b'<p>A closing note.</p><sec><title>References</title><p>R.</p></sec></body></article>')
    d = J.parse_jats(xml)
    assert [(s["title"], s["text"]) for s in d["sections"]] == [("", "An untitled opening."), ("Methods", "M."),
                                                                 ("", "A closing note.")]
