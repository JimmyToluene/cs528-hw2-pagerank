# stage3_pagerank.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Stage 3 — PageRank via power iteration on a sparse stochastic matrix.
#
# References:
#   [1] Page, L., Brin, S., Motwani, R., & Winograd, T. (1999).
#       "The PageRank Citation Ranking: Bringing Order to the Web."
#       http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf
#
#   [2] Langville, A. & Meyer, C. (2004).
#       "A Survey of Eigenvector Methods of Web Information Retrieval."
#       http://citeseer.ist.psu.edu/713792.html
#
# Implementation approach adapted from NetworkX 3.6.1 `_pagerank_scipy`:
#   https://github.com/networkx/networkx/blob/main/networkx/algorithms/link_analysis/pagerank_alg.py
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
# Key ideas borrowed from NetworkX:
#   1. Represent the link graph as a scipy sparse matrix (CSR format)
#      instead of Python dicts — enables C-level vectorized operations.
#   2. Row-normalize the adjacency matrix into a right-stochastic matrix
#      so that A[i][j] = 1/C(i) when page i links to page j.
#   3. Use left matrix-vector multiplication (x @ A) to compute all
#      incoming contributions in one shot, replacing nested Python loops.
#   4. Handle dangling nodes (pages with no outgoing links) by collecting
#      their total rank and redistributing it uniformly to all pages.
#   5. L1-normalize the rank vector after each iteration to prevent
#      floating-point drift from accumulating across iterations.

import numpy as np
import scipy.sparse as sp
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer


def compute_pagerank(outgoing, incoming, damping=0.85, max_iterations=200):
    """
    Compute PageRank using power iteration with sparse matrix operations.

    Implements the original formula from [1]:
        PR(A) = (1-d)/N + d * (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))

    Uses two-phase convergence:
      Phase 1 — Iterate until relative L1 change < 0.5% (homework requirement).
      Phase 2 — Continue until absolute L1 change < N * 1e-6 (NetworkX-level
                precision) so that per-page rankings fully stabilize.

    Why two phases?  The 0.5% threshold can leave residual error that, spread
    across N pages, is enough to shuffle pages with nearly-equal scores.  On
    larger / denser graphs the second eigenvalue of the transition matrix is
    closer to the damping factor, so convergence is slower and more iterations
    are needed to lock in the final ranking order.  [ref: §5 of [2]]

    Args:
        outgoing: page_id (str) -> list of target page_ids
        incoming: page_id (str) -> list of source page_ids (unused in matrix path,
                  kept for API compatibility)
        damping:  damping factor d (default 0.85)
        max_iterations: cap on iteration count

    Returns:
        dict: page_id (str) -> PageRank score (float)
    """
    print_stage("PageRank", "Computing PageRank scores")

    with Timer("Total Stage 3"):
        n = len(outgoing)
        pages = sorted(outgoing.keys(), key=int)
        page_to_idx = {page: i for i, page in enumerate(pages)}

        # ---------------------------------------------------------------
        # Step 1 — Build sparse adjacency matrix  [ref: NetworkX idea #1]
        # ---------------------------------------------------------------
        # A[i][j] = 1 means page i has an outgoing link to page j.
        # We only add edges whose target exists in our page set.
        #
        # Deduplicate: if page A links to page B multiple times in the HTML,
        # it counts as one edge (weight 1), matching how NetworkX's DiGraph
        # handles repeated add_edge() calls.  Without this, csr_matrix sums
        # duplicate (row, col) pairs, giving repeated links extra weight and
        # producing a different stochastic matrix than NetworkX.
        print_step("Building sparse adjacency matrix...")
        rows, cols = [], []
        for source, targets in outgoing.items():
            src_idx = page_to_idx[source]
            for target in set(targets):  # set() deduplicates per source
                if target in page_to_idx:
                    rows.append(src_idx)
                    cols.append(page_to_idx[target])

        data = np.ones(len(rows), dtype=np.float64)
        A = sp.csr_matrix((data, (rows, cols)), shape=(n, n))
        print_success(f"Matrix: {n} nodes, {len(rows)} unique edges")

        # ---------------------------------------------------------------
        # Step 2 — Row-normalize into stochastic matrix  [ref: idea #2]
        # ---------------------------------------------------------------
        # Divide each row by its sum (= out-degree of that page).
        # After this, A[i][j] = 1/C(i) if page i links to page j.
        # This encodes the "PR(Ti)/C(Ti)" term from the formula.
        out_degree = np.array(A.sum(axis=1)).flatten()       # row sums
        is_dangling = np.where(out_degree == 0)[0]           # save before modifying
        out_degree[out_degree == 0] = 1.0                    # avoid /0 for dangling
        D_inv = sp.diags(1.0 / out_degree)
        A = D_inv @ A                                        # row-normalized

        # ---------------------------------------------------------------
        # Step 3 — Prepare initial rank vector and uniform distribution
        # ---------------------------------------------------------------
        # Uniform start: every page begins with PR = 1/N  [ref: formula in [1]]
        x = np.full(n, 1.0 / n, dtype=np.float64)

        # Uniform teleport vector: (1-d)/N applied to every page
        teleport = np.full(n, (1.0 - damping) / n, dtype=np.float64)

        # ---------------------------------------------------------------
        # Step 4 — Power iteration  [ref: ideas #3, #4, #5]
        # ---------------------------------------------------------------
        # Each iteration computes:
        #   x_new = (1-d)/N  +  d * (x @ A  +  dangling_sum / N)
        #
        #   x @ A          — left-multiply: for each page j, sums up
        #                    x[i] * A[i][j] = PR(i)/C(i) over all i→j.
        #                    This is the vectorized form of Σ PR(Ti)/C(Ti).
        #
        #   dangling_sum   — total rank held by dangling pages (no out-links).
        #                    Redistributed uniformly to all N pages, so each
        #                    gets dangling_sum/N.  [ref: dangling handling in [1]]
        #
        #   (1-d)/N        — random-surfer teleport probability.  [ref: [1] §2.1]
        #
        # Two convergence thresholds:
        #   hw_tol   = 0.005  — relative L1 change < 0.5% (homework requirement)
        #   fine_tol = N*1e-6 — absolute L1 (matches NetworkX precision)
        hw_tol = 0.005
        fine_tol = n * 1.0e-6
        hw_converged_at = None

        print_step("Running power iterations...")
        for iteration in range(max_iterations):
            x_prev = x.copy()

            dangling_sum = x[is_dangling].sum()
            x = damping * (x @ A + dangling_sum / n) + teleport

            # L1-normalize: keep sum(x) == 1.0 to prevent floating-point drift
            # across many iterations.  [ref: idea #5]
            x /= x.sum()

            # Convergence metrics
            diff = np.abs(x - x_prev).sum()
            change_pct = diff / x_prev.sum()

            # Phase 1: log when document required 0.5% threshold is first met
            if hw_converged_at is None and change_pct < hw_tol:
                hw_converged_at = iteration + 1
                print_success(f"Homework 0.5% threshold met at iteration {hw_converged_at}")

            # Phase 2: stop when NetworkX-level precision is reached
            if diff < fine_tol:
                print_success(f"Fine convergence (N*1e-6) after {iteration + 1} iterations")
                break

        if hw_converged_at is None:
            print_step(f"Warning: did not reach 0.5% threshold in {max_iterations} iterations")

        print_step(f"Final diff={diff:.10f}, change={change_pct:.6%}")

        # ---------------------------------------------------------------
        # Step 5 — Map back to page IDs and report top 5
        # ---------------------------------------------------------------
        pr = {pages[i]: float(x[i]) for i in range(n)}

        top5 = sorted(pr.items(), key=lambda item: item[1], reverse=True)[:5]
        print_summary_box("Top 5 Pages by PageRank", {
            f"Page {page}": f"{score:.8f}" for page, score in top5
        })

    return pr
