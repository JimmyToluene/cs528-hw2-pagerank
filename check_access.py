# check_access.py
#
# Quick check: verify that you can access the GCS bucket and list HTML files.
#
# Usage:
#   python check_access.py
#   python check_access.py --bucket cs528-hw2-jimmyjia --prefix generated_htmls/

import argparse
from google.cloud import storage

def main():
    parser = argparse.ArgumentParser(description="Check GCS bucket access")
    parser.add_argument('--bucket', default='cs528-hw2-jimmyjia')
    parser.add_argument('--prefix', default='generated_htmls/')
    args = parser.parse_args()

    print(f"Bucket:  {args.bucket}")
    print(f"Prefix:  {args.prefix}")
    print()

    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(args.bucket)

    blobs = list(bucket.list_blobs(prefix=args.prefix, max_results=5))
    if not blobs:
        print("ERROR: No files found. Check bucket name and prefix.")
        return

    print(f"OK — Found files. Showing first {len(blobs)}:")
    for b in blobs:
        print(f"  {b.name}  ({b.size} bytes)")

    # Try reading one file
    sample = blobs[0]
    data = sample.download_as_text()
    print(f"\nSample read ({sample.name}): {len(data)} chars, starts with: {data[:80]}...")
    print("\nAll checks passed. Bucket is accessible.")

if __name__ == "__main__":
    main()
