# main.py

import argparse
import pipeline_pagerank.stage1_read_from_gcs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None, help="Limit number of files to download (for testing)")
    parser.add_argument('--bucket', default='cs528-hw2-jimmyjia')
    parser.add_argument('--prefix', default='generated_htmls/')
    args = parser.parse_args()

    # Stage 1
    outgoing = pipeline_pagerank.stage1_read_from_gcs.read_gcs_files(args.bucket, args.prefix)

    # Quick sanity checks
    print(f"Total files: {len(outgoing)}")
    print(f"Total outgoing links: {sum(len(v) for v in outgoing.values())}")
    print(f"Files with zero links: {sum(1 for v in outgoing.values() if len(v) == 0)}")

    # Show first 5 pages sorted by page ID
    for page_id in sorted(outgoing.keys(), key=lambda x: int(x))[:5]:
        print(f"Page {page_id}: {len(outgoing[page_id])} links → {outgoing[page_id][:5]}...")


if __name__ == "__main__":
    main()