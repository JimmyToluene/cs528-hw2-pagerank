# stage1_read_from_gcs.py
# Reference: Google Cloud Storage Python Client - transfer_manager module
# https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager
# https://cloud.google.com/storage/docs/downloading-objects#download-object-python

import re
import os
import io
from google.cloud import storage
from google.cloud.storage import transfer_manager
from .utils import print_stage, print_step, print_success, print_summary_box, Timer


def parse_html(text):
    """Extract link targets from HTML content."""
    return re.findall(r'<a HREF="(\d+)\.html"', text)


def read_gcs_files(bucket_name, prefix="generated_htmls/"):
    """
    Read and parse all HTML files from a GCS bucket concurrently.

    Uses google.cloud.storage.transfer_manager.download_many() to perform
    parallel bulk downloads into in-memory BytesIO buffers, then parses
    each file to extract outgoing link targets.

    Args:
        bucket_name (str): Name of the GCS bucket (e.g., 'cs528-hw2-jimmyjia')
        prefix (str): Folder prefix within the bucket (e.g., 'generated_htmls/')

    Returns:
        dict: Mapping of page_id (str) -> list of outgoing link target IDs (list[str])
              Example: {'0': ['3349', '8918', '7696'], '1': ['42', '100'], ...}

    Reference:
        https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager
        https://cloud.google.com/storage/docs/downloading-objects#download-object-python
    """
    print_stage("Read", "Parse HTML files read from GCS")

    with Timer("Total Stage 1") as total:

        # --- Step 1: Connect to GCS and list all HTML blobs ---
        print_step(f"Connecting to bucket: {bucket_name}")
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Filter out folder blobs, keep only .html files
        with Timer("Listing blobs"):
            blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith('.html')][:100]
        print_success(f"Found {len(blobs)} HTML files")

        # --- Step 2: Bulk download all blobs into memory ---
        # Each blob's content is written to its own BytesIO buffer
        # Avoids writing to disk and keeps everything in memory
        file_objects = [io.BytesIO() for _ in blobs]

        # Determine concurrency level based on CPU count
        # For I/O-bound tasks, slightly more workers than CPU cores is optimal
        max_workers = min(32, (os.cpu_count() or 4) + 4)
        print_step(f"Downloading {len(blobs)} files with {max_workers} workers...")

        # Create pairs of (blob, BytesIO) as the API expects
        blob_file_pairs = list(zip(blobs, file_objects))
        with Timer("Download"):
            transfer_manager.download_many(
                blob_file_pairs,
                max_workers=max_workers,
                worker_type='thread',  # thread is better for I/O-bound tasks
                raise_exception=True
            )

        # --- Step 3: Parse each downloaded file to extract outgoing links ---
        print_step("Parsing HTML files for links...")
        with Timer("Parsing"):
            outgoing = {}
            for blob, fobj in zip(blobs, file_objects):
                # Extract numeric page ID from blob name
                # e.g., 'generated_htmls/42.html' -> '42'
                page_id = blob.name.split('/')[-1].replace('.html', '')

                # Decode bytes to string, then extract all link targets
                content = fobj.getvalue().decode('utf-8')
                outgoing[page_id] = parse_html(content)

        # --- Summary ---
        total_links = sum(len(v) for v in outgoing.values())
        print_summary_box("Stage 1 Summary", {
            "Files parsed": len(outgoing),
            "Total outgoing links": total_links,
            "Avg links/file": f"{total_links / len(outgoing):.1f}",
        })

    return outgoing