# main.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Entry point for the PageRank pipeline. Reads HTML files from a GCS bucket,
#   computes link statistics, runs iterative PageRank, and validates results
#   against NetworkX.
#
# References:
#   [1] Page, L., Brin, S., Motwani, R., & Winograd, T. (1999).
#       "The PageRank Citation Ranking: Bringing Order to the Web."
#       http://dbpubs.stanford.edu:8090/pub/showDoc.Fulltext?lang=en&doc=1999-66&format=pdf

import argparse

import pipeline_pagerank.stage1_read_from_gcs
import pipeline_pagerank.stage2_stats
import pipeline_pagerank.stage3_pagerank
import pipeline_pagerank.stage4_validation
import pipeline_pagerank.utils as utils

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None, help="Limit number of files to download (for testing)")
    parser.add_argument('--bucket', default='cs528-hw2-jimmyjia')
    parser.add_argument('--prefix', default='generated_htmls/')
    parser.add_argument('--method', default='thread_pool',
                        choices=['thread_pool', 'transfer_manager', 'sequential', 'gcloud'],
                        help="Download strategy (default: thread_pool)")
    args = parser.parse_args()

    utils.print_project_banner()

    # Stage 1
    outgoing = pipeline_pagerank.stage1_read_from_gcs.read_gcs_files(
        args.bucket, args.prefix, method=args.method, limit=args.limit
    )
    # utils.print_dict_sanity_check(outgoing, "Outgoing")

    # Stage 2
    incoming, outgoing_stats, incoming_stats = pipeline_pagerank.stage2_stats.run_stats(outgoing)
    # utils.print_dict_sanity_check(incoming, "Incoming")

    # Stage 3
    pr = pipeline_pagerank.stage3_pagerank.compute_pagerank(outgoing, incoming)

    # Stage 4
    pipeline_pagerank.stage4_validation.verify_with_networkx(outgoing, pr)

if __name__ == "__main__":
    main()