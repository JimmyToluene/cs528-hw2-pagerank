# test_local.py

from pipeline_pagerank.stage1_read_from_gcs import parse_html
from pipeline_pagerank.stage2_stats import run_stats
from pipeline_pagerank.stage3_pagerank import compute_pagerank, verify_with_networkx
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer
import os


def read_local_files(directory, limit=None):
    print_stage("Read", "Parse HTML files read from GCS")
    with Timer("Total Stage 1") as total:
        """Read HTML files from local directory for testing."""
        outgoing = {}
        files = [f for f in os.listdir(directory) if f.endswith('.html')]
        if limit:
            files = files[:limit]

        for fname in files:
            page_id = fname.replace('.html', '')
            with open(os.path.join(directory, fname), 'r') as f:
                outgoing[page_id] = parse_html(f.read())

        print(f"Read {len(outgoing)} local files")
        return outgoing


if __name__ == "__main__":
    # Generate test files first:
    # python generate-content.py -n 100 -m 20

    outgoing = read_local_files("./random_generated", limit=20000)

    # Stage 2
    incoming, outgoing_stats, incoming_stats = run_stats(outgoing)

    # Stage 3
    pr = compute_pagerank(outgoing, incoming)
    verify_with_networkx(outgoing, pr)