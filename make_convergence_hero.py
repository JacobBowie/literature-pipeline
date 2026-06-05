"""
Generate the literature-pipeline case-study hero figure.

Reads _references/convergence_log.csv, aggregates per-project candidate counts
to a portfolio-level trajectory by date, and renders a 2:1 PNG suitable for
both the listing card thumbnail and the case-study hero.

Project names are NOT plotted (OPSEC) — the line is the portfolio total,
annotated at convergence points.
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

NAVY = "#3C5488"
MUTED = "#6b7280"
TEXT = "#1f2937"
LIGHT_BG = "#ffffff"

LOG = Path.home() / "Projects/_references/convergence_log.csv"
OUT = Path.home() / "Projects/jacobbowie-site/img/lit-pipeline-convergence.png"


def getpaid_trajectory(df: pd.DataFrame) -> pd.DataFrame:
    """Hand-curated 5-beat narrative for the getpaid Tier 1 corpus.

      0. Seed set (pre-snowball)
      1. Pass A iter 1 (initial expansion) — May 5
      2. Pass A iter 2 (first convergence) — May 5
      3. Pass B iter 1 (new seeds added → second expansion) — May 13
      4. Pass B iter 2 (second convergence) — May 19

    Drops the redundant rerun on May 6 (re-confirmed Pass A) and the
    May 20 no-op (re-confirmed Pass B). Only getpaid is plotted; it's
    the only public-OK project codename and matches the case study's
    ~17k-DOI scale."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    g = df[df["project"] == "getpaid"].sort_values(["date", "iter"]).reset_index(drop=True)

    # Select the canonical 5-beat narrative rows by (date, iter)
    canonical_keys = [
        (pd.Timestamp("2026-05-05"), 1),  # Pass A iter 1
        (pd.Timestamp("2026-05-05"), 2),  # Pass A iter 2 (converged)
        (pd.Timestamp("2026-05-13"), 1),  # Pass B iter 1
        (pd.Timestamp("2026-05-19"), 1),  # Pass B iter 2 (converged)
    ]
    kept = []
    for date, iter_n in canonical_keys:
        m = g[(g["date"] == date) & (g["iter"] == iter_n)]
        if not m.empty:
            kept.append(m.iloc[0])
    kept = pd.DataFrame(kept).reset_index(drop=True)

    rows = [{"step": 0, "date": kept.iloc[0]["date"], "iter": 0,
             "n": int(kept.iloc[0]["n_before"]), "growth_pct": 0.0,
             "label": "Seed set\n(pre-snowball)"}]
    pass_letters = ["A", "A", "B", "B"]
    for i, r in kept.iterrows():
        rows.append({"step": i + 1, "date": r["date"], "iter": int(r["iter"]),
                     "n": int(r["n_after"]), "growth_pct": float(r["growth_pct"]),
                     "label": f"Pass {pass_letters[i]}\niter {int(r['iter'])}\n{r['date'].strftime('%b %d')}"})
    return pd.DataFrame(rows)


def render(traj: pd.DataFrame, out_path: Path):
    plt.rcParams.update({
        "font.family": ["DejaVu Sans", "Arial"],
        "font.size": 11,
        "axes.edgecolor": "#cbd5e1",
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "ytick.color": TEXT,
        "xtick.color": TEXT,
        "axes.labelcolor": TEXT,
        "text.color": TEXT,
    })

    fig, ax = plt.subplots(figsize=(12, 6), dpi=150, facecolor=LIGHT_BG)

    x = traj["step"]
    y = traj["n"]
    ax.plot(x, y, color=NAVY, lw=2.6, marker="o", markersize=8,
            markerfacecolor=NAVY, markeredgecolor="white", markeredgewidth=1.6,
            zorder=3)
    ax.fill_between(x, 0, y, color=NAVY, alpha=0.08, zorder=1)

    ax.set_xticks(x)
    ax.set_xticklabels(traj["label"].tolist(), fontsize=9, color=TEXT)

    # Annotate growth percentages above the markers (skip seed)
    for i, r in traj.iterrows():
        if r["step"] == 0:
            continue
        label = f"+{r['growth_pct']:.1f}%" if r['growth_pct'] >= 0.1 else "converged"
        color_for = TEXT if r['growth_pct'] >= 1.0 else MUTED
        ax.annotate(label,
                    xy=(r["step"], r["n"]),
                    xytext=(0, 14), textcoords="offset points",
                    ha="center", fontsize=9.5, color=color_for,
                    fontweight="bold" if r['growth_pct'] >= 1.0 else "normal")

    # Final-value callout
    last = traj.iloc[-1]
    ax.annotate(
        f"{int(last['n']):,} DOIs\nin candidate index",
        xy=(last["step"], last["n"]),
        xytext=(-110, -45), textcoords="offset points",
        fontsize=10, color=TEXT, fontweight="bold",
        arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.8, alpha=0.6),
    )

    ax.set_xlabel("")
    ax.set_ylabel("Candidate DOIs in corpus", fontsize=11)
    ax.set_title("Citation snowball convergence on a Tier 1 systematic-review corpus",
                 fontsize=13.5, fontweight="bold", color=TEXT, loc="left", pad=14)
    ax.grid(axis="y", linestyle="--", color="#e5e7eb", lw=0.7, zorder=0)

    # Y-axis padding above max
    ax.set_ylim(0, max(y) * 1.22)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))

    # Footer caption
    fig.text(0.99, 0.02,
             "Each pass is one snowball-orchestrator run: forward + reverse citations + abstracts + reindex.\n"
             "A pass terminates when iteration growth drops below 1%. New seed papers added to the queue\n"
             "produce a fresh expansion in the next pass.",
             ha="right", va="bottom", fontsize=8, color=MUTED, style="italic")

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, facecolor=LIGHT_BG, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    df = pd.read_csv(LOG)
    traj = getpaid_trajectory(df)
    print("Snowball trajectory:")
    print(traj.to_string(index=False))
    render(traj, OUT)
    print(f"\nWrote: {OUT}")
