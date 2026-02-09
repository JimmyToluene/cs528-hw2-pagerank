# main.py

import argparse

import pipeline_pagerank.stage1_read_from_gcs
import pipeline_pagerank.stage2_stats
import pipeline_pagerank.stage3_pagerank
import pipeline_pagerank.utils as utils

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None, help="Limit number of files to download (for testing)")
    parser.add_argument('--bucket', default='cs528-hw2-jimmyjia')
    parser.add_argument('--prefix', default='generated_htmls/')
    args = parser.parse_args()

    # Stage 1
    outgoing = pipeline_pagerank.stage1_read_from_gcs.read_gcs_files(args.bucket, args.prefix)
    utils.print_dict_sanity_check(outgoing, "Outgoing")

    # Stage 2
    incoming, outgoing_stats, incoming_stats = pipeline_pagerank.stage2_stats.run_stats(outgoing)
    utils.print_dict_sanity_check(incoming, "Incoming")

    # Stage 3
    pr = pipeline_pagerank.stage3_pagerank.compute_pagerank(outgoing, incoming)

if __name__ == "__main__":
    main()