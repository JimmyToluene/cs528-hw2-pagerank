# stage3_pagerank.py

from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer
import networkx as nx

def compute_pagerank(outgoing, incoming, damping=0.85, max_iterations=100):
    """
    Compute PageRank using the iterative algorithm.

    PR(A) = (1-d)/n + d * (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))

    Args:
        outgoing (dict): page_id -> list of outgoing link targets
        incoming (dict): page_id -> list of incoming link sources
        damping (float): Damping factor (0.85)
        max_iterations (int): Maximum iterations before stopping

    Returns:
        dict: page_id -> pagerank score
    """
    print_stage("PageRank", "Computing PageRank scores")

    with Timer("Total Stage 3"):
        n = len(outgoing)

        # Step 1: Initialize PR — total sums to 1.0
        pr = {page: 1.0 / n for page in outgoing}

        # Step 2: Precompute outgoing link counts to avoid repeated len() calls
        out_count = {page: len(links) for page, links in outgoing.items()}

        # Step 3: Iterate until convergence
        for iteration in range(max_iterations):
            new_pr = {}

            # Handle dangling nodes
            dangling_sum = sum(pr[page] for page in outgoing if out_count[page] == 0)

            for page in outgoing:
                rank_sum = 0.0
                for source in incoming[page]:
                    if out_count[source] > 0:
                        rank_sum += pr[source] / out_count[source]

                new_pr[page] = (1 - damping) / n + damping * (rank_sum + dangling_sum / n)

            # Convergence: sum of absolute differences across all pages
            diff = sum(abs(new_pr[page] - pr[page]) for page in pr)
            old_sum = sum(pr.values())
            change_pct = diff / old_sum

            pr = new_pr
            print_step(f"Iteration {iteration + 1}: diff={diff:.8f}, change={change_pct:.4%}")

            if change_pct < 0.005:
                print_success(f"Converged after {iteration + 1} iterations")
                break

        # Step 4: Output top 5 pages
        top5 = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:5]
        print_summary_box("Top 5 Pages by PageRank", {
            f"Page {page}": f"{score:.8f}" for page, score in top5
        })

    return pr


def verify_with_networkx(outgoing, custom_pr):
    """
    Verify custom PageRank implementation against NetworkX's built-in PageRank.

    Args:
        outgoing (dict): page_id -> list of outgoing link targets
        custom_pr (dict): page_id -> custom PageRank score
    """
    print_stage("Verify", "Comparing with NetworkX PageRank")

    with Timer("NetworkX verification"):
        # Build directed graph
        print_step("Building NetworkX DiGraph...")
        G = nx.DiGraph()
        for source, targets in outgoing.items():
            for target in targets:
                if target in outgoing:  # only add edges to existing pages
                    G.add_edge(source, target)
        print_success(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

        # Compute NetworkX PageRank
        print_step("Computing NetworkX PageRank...")
        nx_pr = nx.pagerank(G, alpha=0.85)

        # Compare top 5
        nx_top5 = sorted(nx_pr.items(), key=lambda x: x[1], reverse=True)[:5]
        custom_top5 = sorted(custom_pr.items(), key=lambda x: x[1], reverse=True)[:5]

        print_summary_box("NetworkX Top 5", {
            f"Page {page}": f"{score:.8f}" for page, score in nx_top5
        })

        print_summary_box("Custom Top 5", {
            f"Page {page}": f"{score:.8f}" for page, score in custom_top5
        })

        # Compare if same pages appear in top 5
        nx_top5_pages = set(p for p, _ in nx_top5)
        custom_top5_pages = set(p for p, _ in custom_top5)
        overlap = nx_top5_pages & custom_top5_pages

        print_step(f"Top 5 overlap: {len(overlap)}/5 pages match")

        if overlap == nx_top5_pages:
            print_success("Top 5 pages match perfectly!")
        else:
            print_step(f"Only in NetworkX: {nx_top5_pages - custom_top5_pages}")
            print_step(f"Only in Custom: {custom_top5_pages - nx_top5_pages}")