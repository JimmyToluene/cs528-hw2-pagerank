# stage4_validation.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Stage 4 — Validate custom PageRank against NetworkX using standard
#   ranking metrics (Spearman's rho, Kendall's tau, MAE, Precision@5).
#
# References:
#   [1] Spearman, C. (1904).
#       "The Proof and Measurement of Association between Two Things."
#       American Journal of Psychology, 15(1), 72-101.
#       — Spearman's rho: rank-order correlation across all pages.
#
#   [2] Kendall, M. (1938).
#       "A New Measure of Rank Correlation."
#       Biometrika, 30(1/2), 81-93.
#       — Kendall's tau: fraction of concordant pairwise comparisons.
#
#   [3] Kumar, R. & Vassilvitskii, S. (2010).
#       "Generalized Distances between Rankings."
#       WWW 2010.  https://theory.stanford.edu/~sergei/papers/www10-metrics.pdf
#       — Shows Kendall tau and Spearman footrule are equivalent up to
#         a factor of 2 for measuring distance between permutations.
#
# NetworkX License (3-clause BSD):
#   Copyright (c) 2004-2025, NetworkX Developers
#   Aric Hagberg <hagberg@lanl.gov>
#   Dan Schult <dschult@colgate.edu>
#   Pieter Swart <swart@lanl.gov>
#   All rights reserved.
#   Redistribution and use in source and binary forms, with or without
#   modification, are permitted provided that the above copyright notice,
#   this list of conditions, and the following disclaimer are retained.
#   See full license: https://github.com/networkx/networkx/blob/main/LICENSE.txt
#
#   This file invokes nx.DiGraph() and nx.pagerank() at runtime as a
#   reference implementation to validate our custom PageRank results.
#
# Metrics used:
#   Score-level:  Mean Absolute Error (MAE) of per-page PageRank values.
#   Rank-level:   Spearman's rho and Kendall's tau over all N pages.
#   Top-K level:  Precision@5 (set overlap) and positional rank match.

import os
import numpy as np
from scipy.stats import spearmanr, kendalltau, rankdata
import matplotlib
matplotlib.use('Agg')  # non-interactive backend for saving to file
import matplotlib.pyplot as plt
import networkx as nx
from pipeline_pagerank.utils import (
    print_stage, print_step, print_success, print_summary_box,
    print_side_by_side_boxes, Timer,
)

DOCS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs')


def _plot_validation(custom_scores, nx_scores, rho, tau, out_dir):
    """
    Generate two side-by-side plots and save to docs/.

    Left  — Rank vs Rank scatter (Spearman's rho [1]):
            Each point is a page. X = NetworkX rank, Y = Custom rank.
            Points on the diagonal mean identical ranking.

    Right — Score vs Score scatter (Kendall's tau [2]):
            X = NetworkX PageRank score, Y = Custom PageRank score.
            Points on the y=x line mean identical scores.
            Concordant pairs (same relative order) contribute to tau > 0.
    """
    # Compute ranks (1 = highest PR score)
    custom_ranks = rankdata(-custom_scores, method='ordinal')
    nx_ranks = rankdata(-nx_scores, method='ordinal')

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # --- Left: Rank vs Rank (Spearman) ---
    ax1.scatter(nx_ranks, custom_ranks, s=1, alpha=0.3, c='steelblue')
    rank_max = max(custom_ranks.max(), nx_ranks.max())
    ax1.plot([1, rank_max], [1, rank_max], 'r--', linewidth=1, label='Perfect agreement')
    ax1.set_xlabel('NetworkX Rank')
    ax1.set_ylabel('Custom Rank')
    ax1.set_title(f'Rank vs Rank  (Spearman ρ = {rho:.6f})')
    ax1.legend(loc='upper left')
    ax1.set_aspect('equal')

    # --- Right: Score vs Score (Kendall) ---
    ax2.scatter(nx_scores, custom_scores, s=1, alpha=0.3, c='darkorange')
    score_min = min(nx_scores.min(), custom_scores.min())
    score_max = max(nx_scores.max(), custom_scores.max())
    ax2.plot([score_min, score_max], [score_min, score_max], 'r--', linewidth=1, label='y = x')
    ax2.set_xlabel('NetworkX PageRank Score')
    ax2.set_ylabel('Custom PageRank Score')
    ax2.set_title(f'Score vs Score  (Kendall τ = {tau:.6f})')
    ax2.legend(loc='upper left')
    ax2.set_aspect('equal')

    fig.suptitle('Custom PageRank vs NetworkX PageRank Validation', fontsize=14, fontweight='bold')
    fig.tight_layout()

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, 'validation_rank_correlation.png')
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    return path


def verify_with_networkx(outgoing, custom_pr):
    """
    Verify custom PageRank against NetworkX's built-in PageRank.

    Reports four categories of agreement metrics:
      1. Score-level  — MAE and max absolute error across all pages.
      2. Rank-level   — Spearman's rho [1] and Kendall's tau [2] computed
                        on the full ranking of all N pages.
      3. Top-5 display — side-by-side ranked lists for visual inspection.
      4. Top-5 stats  — Precision@5 (set overlap) and positional match.
      5. Figures      — Rank-vs-rank and score-vs-score scatter plots
                        saved to docs/validation_rank_correlation.png.

    Args:
        outgoing (dict): page_id -> list of outgoing link targets
        custom_pr (dict): page_id -> custom PageRank score
    """
    print_stage("Verify", "Comparing with NetworkX PageRank")

    with Timer("NetworkX verification"):
        # --- Build NetworkX graph and compute reference PageRank ---
        print_step("Building NetworkX DiGraph...")
        G = nx.DiGraph()
        for source, targets in outgoing.items():
            for target in targets:
                if target in outgoing:
                    G.add_edge(source, target)
        print_success(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

        print_step("Computing NetworkX PageRank...")
        nx_pr = nx.pagerank(G, alpha=0.85)

        # --- Align scores into parallel arrays (same page order) ---
        pages = sorted(custom_pr.keys(), key=int)
        custom_scores = np.array([custom_pr[p] for p in pages])
        nx_scores = np.array([nx_pr.get(p, 0.0) for p in pages])

        # =============================================================
        # Metric 1 — Score-level comparison (MAE, Max Error)
        # =============================================================
        abs_errors = np.abs(custom_scores - nx_scores)
        mae = abs_errors.mean()
        max_err = abs_errors.max()
        max_err_page = pages[abs_errors.argmax()]

        # =============================================================
        # Metric 2 — Rank-level comparison  [ref: [1], [2], [3]]
        # =============================================================
        # Spearman's rho: correlation between rank orderings.
        #   +1 = identical ranking, 0 = no correlation, -1 = reversed.
        rho, rho_p = spearmanr(custom_scores, nx_scores)

        # Kendall's tau: proportion of concordant vs discordant pairs.
        #   +1 = all pairs agree, 0 = random, -1 = all pairs disagree.
        tau, tau_p = kendalltau(custom_scores, nx_scores)

        print_summary_box("Validation Metrics", {
            "MAE (score)": f"{mae:.2e}",
            "Max error":   f"{max_err:.2e} (Page {max_err_page})",
            "Spearman rho [1]": f"{rho:.6f} (p={rho_p:.2e})",
            "Kendall tau  [2]": f"{tau:.6f} (p={tau_p:.2e})",
        })

        # =============================================================
        # Metric 3 — Top-5 side-by-side display
        # =============================================================
        nx_top5 = sorted(nx_pr.items(), key=lambda x: x[1], reverse=True)[:5]
        custom_top5 = sorted(custom_pr.items(), key=lambda x: x[1], reverse=True)[:5]

        custom_display = {f"#{i+1} Page {p}": f"{s:.8f}" for i, (p, s) in enumerate(custom_top5)}
        nx_display = {f"#{i+1} Page {p}": f"{s:.8f}" for i, (p, s) in enumerate(nx_top5)}

        print_side_by_side_boxes(
            "Custom Top 5", custom_display,
            "NetworkX Top 5", nx_display,
        )

        # =============================================================
        # Metric 4 — Top-5 rank agreement
        # =============================================================
        nx_top5_pages = [p for p, _ in nx_top5]
        custom_top5_pages = [p for p, _ in custom_top5]

        # Positional match: same page at same rank position
        rank_matches = sum(1 for a, b in zip(custom_top5_pages, nx_top5_pages) if a == b)
        # Precision@5: how many of top-5 pages appear in both lists
        overlap = set(nx_top5_pages) & set(custom_top5_pages)

        if rank_matches == 5:
            print_success("Top 5 matches perfectly (same pages, same order)")
        else:
            print_step(f"Top 5 positional match: {rank_matches}/5")
            print_step(f"Top 5 Precision@5:      {len(overlap)}/5")

        # =============================================================
        # Metric 5 — Scatter plots  [ref: visual validation]
        # =============================================================
        plot_path = _plot_validation(custom_scores, nx_scores, rho, tau, DOCS_DIR)
        print_success(f"Scatter plots saved to {plot_path}")
