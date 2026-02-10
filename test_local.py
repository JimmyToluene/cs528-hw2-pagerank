# test_local.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Local test runner — reads HTML files from a local directory (instead of
#   GCS) and runs the full pipeline (stats, PageRank, validation) for
#   development and debugging.

from concurrent.futures import ThreadPoolExecutor
from pipeline_pagerank.stage1_read_from_gcs import parse_html
from pipeline_pagerank.stage2_stats import run_stats
from pipeline_pagerank.stage3_pagerank import compute_pagerank
from pipeline_pagerank.stage4_validation import verify_with_networkx
from pipeline_pagerank.utils import print_project_banner, print_stage, print_step, print_success, print_summary_box, Timer
import os


def _read_and_parse(filepath):
    """Read a single HTML file and return (page_id, links)."""
    page_id = os.path.basename(filepath).replace('.html', '')
    with open(filepath, 'r') as f:
        return page_id, parse_html(f.read())


def read_local_files(directory, limit=None):
    """Read HTML files from local directory using parallel I/O."""
    print_stage("Read", "Parse HTML files from local directory")

    with Timer("Total Stage 1"):
        # Collect file paths
        files = [f for f in os.listdir(directory) if f.endswith('.html')]
        if limit:
            files = files[:limit]
        filepaths = [os.path.join(directory, f) for f in files]

        # Parallel read + parse (mirrors stage1's threaded GCS downloads)
        max_workers = min(32, (os.cpu_count() or 4) + 4)
        print_step(f"Reading {len(filepaths)} files with {max_workers} threads...")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            outgoing = dict(pool.map(_read_and_parse, filepaths))

        # Summary
        total_links = sum(len(v) for v in outgoing.values())
        print_summary_box("Local Read Summary", {
            "Files parsed": len(outgoing),
            "Total outgoing links": total_links,
            "Avg links/file": f"{total_links / len(outgoing):.1f}",
        })

    return outgoing


if __name__ == "__main__":
    # Generate test files first:
    # python generate-content.py -n 20000 -m 375

    print_project_banner()
    outgoing = read_local_files("./random_generated", limit=20000)

    # Stage 2
    incoming, outgoing_stats, incoming_stats = run_stats(outgoing)

    # Stage 3
    pr = compute_pagerank(outgoing, incoming)

    # Stage 4
    verify_with_networkx(outgoing, pr)
