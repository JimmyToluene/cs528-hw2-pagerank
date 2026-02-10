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

from pipeline_pagerank.stage1_read import read_files
from pipeline_pagerank.stage2_stats import run_stats
from pipeline_pagerank.stage3_pagerank import compute_pagerank
from pipeline_pagerank.stage4_validation import verify_with_networkx
from pipeline_pagerank.utils import print_project_banner

if __name__ == "__main__":
    # Generate test files first:
    # python generate-content.py -n 20000 -m 375

    print_project_banner()

    # read_files auto-detects local directory
    outgoing = read_files("./random_generated", limit=20000)

    # Stage 2
    incoming, outgoing_stats, incoming_stats = run_stats(outgoing)

    # Stage 3
    pr = compute_pagerank(outgoing, incoming)

    # Stage 4
    verify_with_networkx(outgoing, pr)
