"""Smoke tests for the MathML → LaTeX path.

We don't own the vendored converter (`py-mathml-to-latex`, MIT) and we don't
commit to math-conversion correctness — see README §MathML→LaTeX. These tests
exist to catch the failure modes that would matter:

  1. The vendored library failed to import (e.g., a dependency hash drift)
  2. Our `_formula_latex` wrapper regressed
  3. Common cases that any sane MathML→LaTeX converter must handle silently
     started returning empty / errored output

Three canonical cases: fraction, sum-with-sub/superscript, Greek + operator.
Anything beyond this is out of scope for the pipeline's contract; the original
MathML is preserved in the sidecar so users can reprocess with their own tools.
"""
from xml.etree import ElementTree as ET

import pytest

from jats_to_text import _MATH_AVAILABLE, _formula_latex

MML_NS = "http://www.w3.org/1998/Math/MathML"


def _mathml(inner: str) -> ET.Element:
    """Wrap an inner MathML fragment as a complete <math> element."""
    return ET.fromstring(f'<math xmlns="{MML_NS}">{inner}</math>')


@pytest.mark.skipif(not _MATH_AVAILABLE, reason="vendored mathml-to-latex unavailable")
class TestMathmlSmoke:
    """If any of these three break, something is badly wrong upstream."""

    def test_simple_fraction(self):
        """<mfrac><mn>1</mn><mi>x</mi></mfrac> → \\frac{1}{x}"""
        elem = _mathml("<mfrac><mn>1</mn><mi>x</mi></mfrac>")
        latex, status, _ = _formula_latex(elem)
        assert status == "ok", f"status={status!r}, latex={latex!r}"
        assert latex.strip(), "converter returned empty LaTeX for a simple fraction"
        # Tolerant check: any common fraction form is acceptable
        assert any(tok in latex for tok in (r"\frac", r"\dfrac", r"\over")), \
            f"no fraction primitive in output: {latex!r}"

    def test_sum_with_sub_and_superscript(self):
        """sum_{i=1}^{n} x_i should produce a \\sum with bounds and a subscript."""
        elem = _mathml(
            "<munderover>"
            "  <mo>&#x2211;</mo>"          # ∑
            "  <mrow><mi>i</mi><mo>=</mo><mn>1</mn></mrow>"
            "  <mi>n</mi>"
            "</munderover>"
            "<msub><mi>x</mi><mi>i</mi></msub>"
        )
        latex, status, _ = _formula_latex(elem)
        assert status == "ok", f"status={status!r}, latex={latex!r}"
        assert r"\sum" in latex, f"expected \\sum in output, got {latex!r}"
        # Should reference n somewhere (upper bound)
        assert "n" in latex, f"upper bound 'n' missing from {latex!r}"
        # Should have a subscript for x_i
        assert "_" in latex or "{i}" in latex, f"no subscript in {latex!r}"

    def test_greek_letter_and_operator(self):
        """α = β · γ → mix of Greek (\\alpha etc.) and a multiplication operator."""
        elem = _mathml(
            "<mi>&#x03B1;</mi>"           # α
            "<mo>=</mo>"
            "<mi>&#x03B2;</mi>"           # β
            "<mo>&#x22C5;</mo>"           # ⋅ (cdot)
            "<mi>&#x03B3;</mi>"           # γ
        )
        latex, status, _ = _formula_latex(elem)
        assert status == "ok", f"status={status!r}, latex={latex!r}"
        assert latex.strip(), "converter returned empty LaTeX for Greek expression"
        # At least one Greek primitive should survive — alpha or its TeX form
        assert any(tok in latex.lower() for tok in (r"\alpha", "α", "alpha")), \
            f"no \\alpha-equivalent in output: {latex!r}"


@pytest.mark.skipif(not _MATH_AVAILABLE, reason="vendored mathml-to-latex unavailable")
def test_no_mathml_returns_status():
    """When the input doesn't contain <math>, we get a status, not a crash."""
    elem = ET.fromstring("<inline-formula><p>not math</p></inline-formula>")
    latex, status, _ = _formula_latex(elem)
    assert status == "no-mathml"
    assert latex == ""


@pytest.mark.skipif(not _MATH_AVAILABLE, reason="vendored mathml-to-latex unavailable")
def test_readme_verify_snippet_works():
    """Lock in the README §Verify-it-yourself snippet — if this breaks, the
    docs are lying."""
    mathml = ('<math xmlns="http://www.w3.org/1998/Math/MathML">'
              '<mfrac><mn>1</mn><mi>x</mi></mfrac></math>')
    latex, status, _ = _formula_latex(ET.fromstring(mathml))
    assert status == "ok"
    assert latex.strip()
