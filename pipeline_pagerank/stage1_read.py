# stage1_read.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Stage 1 — Read and parse HTML files from either GCS or a local directory.
#
#   Auto-detection:
#     - If `source` is an existing local directory → read from disk.
#     - Otherwise → treat `source` as a GCS bucket name and download from GCS.
#
#   GCS download uses transfer_manager.download_many_to_path() with process-based
#   workers (Google's recommended approach for many small files).  Per Google's
#   benchmark, process workers achieve up to 50x throughput vs single-worker
#   for files under 16KB.  [ref: Google Cloud Blog]
#
# References:
#   [1] transfer_manager API
#       https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager
#   [2] Download many objects (sample)
#       https://cloud.google.com/storage/docs/samples/storage-transfer-manager-download-many
#   [3] Improve throughput with Cloud Storage client libraries
#       https://cloud.google.com/blog/products/storage-data-transfer/improve-throughput-with-cloud-storage-client-libraries/

import re
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer


def parse_html(text):
    """Extract link targets from HTML content."""
    return re.findall(r'<a HREF="(\d+)\.html"', text)


def _read_and_parse(filepath):
    """Read a single HTML file from disk and return (page_id, links)."""
    page_id = os.path.basename(filepath).replace('.html', '')
    with open(filepath, 'r') as f:
        return page_id, parse_html(f.read())


def _parse_directory(directory, limit=None):
    """Parse all HTML files in a directory using threaded I/O."""
    files = sorted(f for f in os.listdir(directory) if f.endswith('.html'))
    if limit:
        files = files[:limit]
    filepaths = [os.path.join(directory, f) for f in files]
    print_success(f"Found {len(filepaths)} HTML files")

    max_workers = min(32, (os.cpu_count() or 4) + 4)
    print_step(f"Parsing with {max_workers} threads...")
    with Timer("Parsing"):
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            outgoing = dict(pool.map(_read_and_parse, filepaths))

    return outgoing


def _read_local(directory, limit=None):
    """Read HTML files from a local directory."""
    print_stage("Read", "Parse HTML files from local directory")

    with Timer("Total Stage 1"):
        outgoing = _parse_directory(directory, limit=limit)

        total_links = sum(len(v) for v in outgoing.values())
        print_summary_box("Stage 1 Summary", {
            "Source": directory,
            "Files parsed": len(outgoing),
            "Total outgoing links": total_links,
            "Avg links/file": f"{total_links / len(outgoing):.1f}",
        })

    return outgoing


def _read_gcs(bucket_name, prefix="generated_htmls/", limit=None):
    """
    Download and parse HTML files from GCS.

    Uses transfer_manager.download_many_to_path() with process-based workers
    (default) for maximum throughput on many small files.  [ref: [1], [3]]

    Process workers bypass Python's GIL and achieve significantly higher
    throughput than threads for I/O-bound bulk downloads.
    """
    from google.cloud import storage
    from google.cloud.storage import transfer_manager

    print_stage("Read", "Parse HTML files from GCS")

    tmp_dir = tempfile.mkdtemp(prefix="pagerank_")
    try:
        with Timer("Total Stage 1"):
            # --- Step 1: Connect and list blobs ---
            print_step(f"Connecting to bucket: {bucket_name}")
            try:
                client = storage.Client()
                print_success("Authenticated client")
            except Exception:
                client = storage.Client.create_anonymous_client()
                print_success("Anonymous client")
            bucket = client.bucket(bucket_name)

            with Timer("Listing blobs"):
                blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith('.html')]
            if limit:
                blobs = blobs[:limit]
            blob_names = [b.name for b in blobs]
            print_success(f"Found {len(blob_names)} HTML files")

            # --- Step 2: Bulk download via transfer_manager ---
            # worker_type="process" (default) — uses multiprocessing, bypasses GIL.
            # max_workers=8 — Google's recommended default.  [ref: [1]]
            print_step(f"Downloading {len(blob_names)} files (process workers)...")
            with Timer("Download"):
                results = transfer_manager.download_many_to_path(
                    bucket,
                    blob_names,
                    destination_directory=tmp_dir,
                    max_workers=8,
                )

            # Check for errors
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                print_step(f"Warning: {len(errors)} download errors")

            # --- Step 3: Parse downloaded files ---
            outgoing = _parse_directory(tmp_dir, limit=limit)

            total_links = sum(len(v) for v in outgoing.values())
            print_summary_box("Stage 1 Summary", {
                "Source": f"gs://{bucket_name}/{prefix}",
                "Files parsed": len(outgoing),
                "Total outgoing links": total_links,
                "Avg links/file": f"{total_links / len(outgoing):.1f}",
            })

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return outgoing


def read_files(source, prefix="generated_htmls/", limit=None, **kwargs):
    """
    Read and parse HTML files. Auto-detects source type:
      - If `source` is an existing local directory → read from disk.
      - Otherwise → treat `source` as a GCS bucket name.

    Args:
        source (str): Local directory path or GCS bucket name
        prefix (str): GCS folder prefix (ignored for local)
        limit (int|None): Max number of files to read

    Returns:
        dict: page_id (str) -> list of outgoing link target IDs
    """
    if os.path.isdir(source):
        print_step(f"Detected local directory: {source}")
        return _read_local(source, limit=limit)
    else:
        print_step(f"Detected GCS bucket: {source}")
        return _read_gcs(source, prefix=prefix, limit=limit)
