# Vendored third-party packages

These packages are copied into the project so it remains reproducible even if the
upstream PyPI distribution is yanked, renamed, or evolves incompatibly. The
project imports from `vendor/` rather than relying on `pip install`.

## mathml-to-latex

- **Upstream**: <https://github.com/asnunes/py-mathml-to-latex>
- **Version**: 1.0.0
- **PyPI release**: 2024-11-09
- **Vendored on**: 2026-04-28
- **License**: MIT (see GitHub LICENSE; PyPI release omits the License classifier)
- **Why vendored**: single release, solo author, 17 months without commits.
  See `tools/PMC_FETCH_README.md` and project memory for the audit notes.

### Local modifications

Changes from upstream are marked in code with a `# VENDORED FIX (date):` comment.

- `xml_to_mathml/services/error_handler.py::_fix_missing_attribute` — repaired a
  stale-variable bug in the regex-substitution loop. Upstream loop variable
  `xml` was never reassigned, so each iteration re-searched the unchanged
  string and the `counter < 5` cap was the only thing preventing infinite
  iteration. Restored the intended iterative-substitution behavior.

- `el_to_tex/usecases.py::GenericSpacingWrapper.convert` — backport of JS
  upstream commit [asnunes/mathml-to-latex@5d1b794](https://github.com/asnunes/mathml-to-latex/commit/5d1b794)
  ("fix: mo + mtable now render cases env instead of raw separators",
  v1.5.0, May 2025). Detects the linear-system pattern
  `{ + mtable + empty closing mo` and renders as `\begin{cases}...\end{cases}`
  instead of falling through the generic spacing wrapper, which would
  otherwise emit raw alignment tabs that fail under pdflatex.

  **Verified by:**
  - **Positive test:** fabricated cases-pattern MathML → produces correct
    `\begin{cases} x + y = 3 \\ x - y = 1 \end{cases}`
  - **Regression test:** all 5 `mtable`-flagged formulas in the live
    corpus (across getpaid + Physiological_Data) produce **identical**
    output before and after the patch — the fix's pattern-detection only
    fires on real cases env, not on single-cell `<mtable>` wrappers
    (the publisher quirk we actually have in our corpus).
  - **Unaffected pattern:** 2x2 matrix `(a b; c d)` with `<mfenced>` was
    already rendered correctly by upstream as `\begin{pmatrix}` — the
    fix is orthogonal.

  **Status:** ready for upstream PR to `asnunes/py-mathml-to-latex` (Path B
  step 2, deferred). Patch is self-contained; tests demonstrate no
  regressions; same maintainer ships both repos so the JS commit is
  authoritative reference.

### Update procedure (if upstream ships v1.1+)

1. Re-download `pip install mathml-to-latex==<new-ver>` into a fresh venv.
2. `diff -r` the vendored copy against the new install.
3. Re-apply the local modifications listed above (or supersede if upstream fixed them).
4. Update this file's "Version" / "Vendored on" / "Local modifications" sections.
5. Re-run `pipeline_check.py` and refresh sidecars with `backfill_fulltext.py --refresh`.

### Upstream gap analysis (audited 2026-04-28)

The Python port (`asnunes/py-mathml-to-latex`) is frozen at v1.0.0 (Nov 2024).
The JS upstream (`asnunes/mathml-to-latex`, **same author**) is active and has
shipped three releases since the Python port forked:

| JS version | Date | Notable changes relevant to our pipeline |
|---|---|---|
| v1.4.2 | 2024-11 | Improved subscript/superscript conversion logic |
| v1.4.3 | 2024-11 | `mmultiscripts` + empty `mprescripts` support |
| v1.5.0 | 2025-05 | Accent mapping corrections; `mfenced` default-separator; **`mo + mtable` now renders `cases` env instead of raw alignment tabs**; `mspace` newline support; `mrow` converter refactor |

The v1.5.0 `mo + mtable` fix directly addresses the matrix bug our parser flags
as `status: "ok-but-matrix-likely-broken"`. Until the Python port catches up,
our flag-it-don't-render-it approach is the right mitigation.

**Why we didn't manually port:** 35 JS commits is too much surface area to
hand-translate without introducing new bugs, and the same maintainer ships both
repos — they're more likely to publish a Python v1.0.1 synced to JS v1.5.0 than
we are to ship a clean port. Manual port → ongoing fork divergence we'd own.

**Recommended action when matrix-rendering becomes load-bearing:** open an
issue on `asnunes/py-mathml-to-latex` asking for a v1.0.1 release synced to
JS v1.5.0. Re-run our 25-case `pdflatex` benchmark against the new version
when it lands; expect the matrix bucket to flip from 0/2 to 2/2.

### Out of scope: do NOT do these without explicit decision

- Don't fix the multi-`<mi>` joining (the "T r e c" cosmetic issue) — it's
  structural in upstream `el_to_tex/usecases.py::Math.convert`. The post-process
  regex in `tools/jats_to_text.py::_formula_latex` already handles the common
  cases. Fixing it in the vendored copy would diverge from upstream substantially.
- Don't fix the matrix-delimiter / function-name bugs — same reason. Either
  switch to a different converter (Pandoc fallback) when those become a real
  problem, or fix them upstream and back-port.
