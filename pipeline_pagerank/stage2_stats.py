# stage2_stats.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Stage 2 — Build incoming link index and compute link statistics
#   (min, max, average, median, quintiles) for both directions.

import numpy as np
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_side_by_side_boxes, Timer


def build_incoming(outgoing):
    """
    Build incoming link dictionary from outgoing links.

    Args:
        outgoing (dict): page_id -> list of target page_ids

    Returns:
        dict: page_id -> list of source page_ids that link to it
    """
    incoming = {page_id: [] for page_id in outgoing}

    for source, targets in outgoing.items():
        for target in targets:
            if target in incoming:
                incoming[target].append(source)

    return incoming


def compute_link_stats(link_counts):
    """
    Compute statistics for a list of link counts.

    Args:
        link_counts (list[int]): Number of links per page

    Returns:
        dict: Computed statistics (all values as strings for display)
    """
    values = np.array(link_counts)

    return {
        "Min": int(np.min(values)),
        "Max": int(np.max(values)),
        "Average": f"{np.mean(values):.2f}",
        "Median": f"{np.median(values):.2f}",
        "Q1 (20th)": f"{np.percentile(values, 20):.2f}",
        "Q2 (40th)": f"{np.percentile(values, 40):.2f}",
        "Q3 (60th)": f"{np.percentile(values, 60):.2f}",
        "Q4 (80th)": f"{np.percentile(values, 80):.2f}",
    }


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

        # Step 2: Compute stats for both directions
        print_step("Computing link statistics...")
        outgoing_counts = [len(v) for v in outgoing.values()]
        incoming_counts = [len(v) for v in incoming.values()]
        outgoing_stats = compute_link_stats(outgoing_counts)
        incoming_stats = compute_link_stats(incoming_counts)

        # Display side by side: [Outgoing] [Incoming]
        print_side_by_side_boxes(
            "Outgoing Link Statistics", outgoing_stats,
            "Incoming Link Statistics", incoming_stats,
        )

    return incoming, outgoing_stats, incoming_stats