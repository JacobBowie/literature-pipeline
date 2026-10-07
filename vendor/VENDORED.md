# Vendored third-party packages

These packages are copied into the project so it remains reproducible even if the
upstream PyPI distribution is yanked, renamed, or evolves incompatibly. The
project imports from `vendor/` rather than relying on `pip install`.

## mathml-to-latex

- **Upstream**: <https://github.com/asnunes/py-mathml-to-latex>
- **Version**: 1.0.0
- **PyPI release**: 2024-11-09
- **Vendored on**: 2026-04-28
- **License**: MIT. The full upstream text is vendored alongside this package at
  `vendor/mathml_to_latex/LICENSE` (the PyPI release omits the License classifier).
- **Why vendored**: a single release from a solo author, 17 months without commits at the
  time of vendoring (audit of 2026-04-28, summarised under "Upstream gap analysis" below).

### Local modifications

Changes from upstream are marked in code with a `# VENDORED FIX (date):` comment.

- `xml_to_mathml/services/error_handler.py::_fix_missing_attribute`: repaired a
  stale-variable bug in the regex-substitution loop. The upstream loop variable
  `xml` was never reassigned, so each iteration re-searched the unchanged
  string and the `counter < 5` cap was the only thing preventing infinite
  iteration. Restored the intended iterative-substitution behavior.

- `el_to_tex/usecases.py::GenericSpacingWrapper.convert`: backport of the JS
  upstream commit [asnunes/mathml-to-latex@5d1b794](https://github.com/asnunes/mathml-to-latex/commit/5d1b794)
  ("fix: mo + mtable now render cases env instead of raw separators",
  v1.5.0, May 2025). Detects the linear-system pattern
  `{ + mtable + empty closing mo` and renders it as `\begin{cases}...\end{cases}`
  instead of falling through the generic spacing wrapper, which would
  otherwise emit raw alignment tabs that fail under pdflatex.

  **Verified by:**
  - **Positive test:** fabricated cases-pattern MathML produces the correct
    `\begin{cases} x + y = 3 \\ x - y = 1 \end{cases}`.
  - **Regression test:** all 5 `mtable`-flagged formulas in two real libraries
    produce **identical** output before and after the patch: the fix's pattern
    detection fires only on a real cases environment, not on single-cell
    `<mtable>` wrappers (the publisher quirk those libraries actually contain).
  - **Unaffected pattern:** a 2x2 matrix `(a b; c d)` with `<mfenced>` was
    already rendered correctly by upstream as `\begin{pmatrix}`; the fix is
    orthogonal.

  **Status:** an upstream pull request to `asnunes/py-mathml-to-latex` is optional and not
  planned for now. The patch is self-contained, the tests show no regressions, and the same
  maintainer ships both repositories, so the JS commit is the authoritative reference.

### Update procedure (if upstream ships v1.1+)

1. Install `mathml-to-latex==<new-ver>` into a fresh, throwaway environment.
2. `diff -r` the vendored copy against the new install.
3. Re-apply the local modifications listed above (or drop the ones upstream fixed).
4. Update this file's "Version", "Vendored on" and "Local modifications" sections.
5. Run the test suite (`tests/test_mathml_smoke.py` covers the converter), then refresh the
   JATS sidecars with `backfill_fulltext.py --lib-dir DIR --refresh`.

### Upstream gap analysis (audited 2026-04-28)

The Python port (`asnunes/py-mathml-to-latex`) is frozen at v1.0.0 (Nov 2024).
The JS upstream (`asnunes/mathml-to-latex`, **same author**) is active and has
shipped three releases since the Python port forked:

| JS version | Date | Notable changes relevant to this pipeline |
|---|---|---|
| v1.4.2 | 2024-11 | Improved subscript/superscript conversion logic |
| v1.4.3 | 2024-11 | `mmultiscripts` + empty `mprescripts` support |
| v1.5.0 | 2025-05 | Accent mapping corrections; `mfenced` default separator; **`mo + mtable` now renders the `cases` environment instead of raw alignment tabs**; `mspace` newline support; `mrow` converter refactor |

The v1.5.0 `mo + mtable` fix directly addresses the matrix bug our parser flags
as `status: "ok-but-matrix-likely-broken"`. Until the Python port catches up,
flagging rather than rendering is the right mitigation.

**Why we didn't port it by hand:** 35 JS commits is too much surface area to
hand-translate without introducing new bugs, and the same maintainer ships both
repositories, so a Python release synced to JS v1.5.0 is more likely to come from
upstream than a clean port from us. A manual port would be an ongoing fork we'd own.

**Recommended action when matrix rendering becomes load-bearing:** open an
issue on `asnunes/py-mathml-to-latex` asking for a release synced to JS v1.5.0.
At that point a real benchmark suite becomes worth building; until then,
MathML-to-LaTeX correctness is an explicit non-goal of this pipeline: the source
MathML is kept in every sidecar (`formulas[i].mathml_input`) so a wrong conversion
can be redone with another tool (README, "Text, OCR, tables and figures").

### Out of scope: do NOT do these without an explicit decision

- Don't fix the multi-`<mi>` joining (the "T r e c" cosmetic issue): it is
  structural in upstream `el_to_tex/usecases.py::Math.convert`. The post-process
  regex in `jats_to_text.py::_formula_latex` already handles the common
  cases. Fixing it in the vendored copy would diverge from upstream substantially.
- Don't fix the matrix-delimiter or function-name bugs, for the same reason. Either
  switch to a different converter (a Pandoc fallback) when those become a real
  problem, or fix them upstream and back-port.
