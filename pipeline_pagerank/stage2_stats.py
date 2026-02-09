import numpy as np
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer


def build_incoming(outgoing):
    """
    Build incoming link dictionary from outgoing links.

    Args:
        outgoing (dict): page_id -> list of target page_ids

    Returns:
        dict: page_id -> list of source page_ids that link to it
    """
    # Initialize all incoming pages with empty lists
    incoming = {page_id: [] for page_id in outgoing}

    for source, targets in outgoing.items():
        for target in targets:
            # Only count links to pages that exist in our dataset
            if target in incoming:
                incoming[target].append(source)

    return incoming

def compute_link_stats(link_counts, label):
    """
    Compute and display statistics for a list of link counts.

    Args:
        link_counts (list[int]): Number of links per page
        label (str): Label for display (e.g., "Outgoing" or "Incoming")

    Returns:
        dict: Computed statistics
    """
    values = np.array(link_counts)

    stats = {
        "Min": int(np.min(values)),
        "Max": int(np.max(values)),
        "Average": f"{np.mean(values):.2f}",
        "Median": f"{np.median(values):.2f}",
        "Q1 (20th)": f"{np.percentile(values, 20):.2f}",
        "Q2 (40th)": f"{np.percentile(values, 40):.2f}",
        "Q3 (60th)": f"{np.percentile(values, 60):.2f}",
        "Q4 (80th)": f"{np.percentile(values, 80):.2f}",
    }

    print_summary_box(f"{label} Link Statistics", stats)
    return stats


def run_stats(outgoing):
    """
    Build incoming links and compute statistics for both directions.

    Args:
        outgoing (dict): page_id -> list of outgoing link targets

    Returns:
        tuple: (incoming dict, outgoing_stats dict, incoming_stats dict)
    """
    print_stage("Stats", "Computing link statistics")

    with Timer("Total Stage 2"):
        # Step 1: Build incoming links from outgoing
        print_step("Building incoming link index...")
        with Timer("Building incoming links"):
            incoming = build_incoming(outgoing)

        # Step 2: Compute outgoing stats
        print_step("Computing outgoing link statistics...")
        outgoing_counts = [len(v) for v in outgoing.values()]
        outgoing_stats = compute_link_stats(outgoing_counts, "Outgoing")

        # Step 3: Compute incoming stats
        print_step("Computing incoming link statistics...")
        incoming_counts = [len(v) for v in incoming.values()]
        incoming_stats = compute_link_stats(incoming_counts, "Incoming")

    return incoming, outgoing_stats, incoming_stats