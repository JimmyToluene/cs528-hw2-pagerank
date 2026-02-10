# stage1_read_from_gcs.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Stage 1 — Read and parse HTML files from a Google Cloud Storage bucket.
#   Two download strategies are available (selectable via `progress` flag):
#
#     progress=False (default, stable):
#       Uses google.cloud.storage.transfer_manager.download_many() for
#       bulk parallel downloads.  Proven and battle-tested.
#
#     progress=True (experimental):
#       Uses a custom ThreadPoolExecutor + tqdm for a live progress bar
#       with ETA.  Replaces download_many() because that API provides no
#       progress callback — it is a single blocking call that only returns
#       when all downloads are complete.
#
# References:
#   [1] Google Cloud Storage Python Client — transfer_manager module
#       https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager
#   [2] Downloading objects from GCS
#       https://cloud.google.com/storage/docs/downloading-objects#download-object-python

import re
import os
import io
import subprocess
import tempfile
import shutil
import time
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from google.cloud.storage import transfer_manager
from tqdm import tqdm
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer


def parse_html(text):
    """Extract link targets from HTML content."""
    return re.findall(r'<a HREF="(\d+)\.html"', text)


def _download_blob(blob):
    """Download a single blob into memory and return (blob_name, bytes)."""
    buf = io.BytesIO()
    blob.download_to_file(buf)
    return blob.name, buf.getvalue()


def _download_stable(blobs, max_workers):
    """
    Stable path — uses transfer_manager.download_many().

    Downloads all blobs in parallel into BytesIO buffers using the official
    GCS transfer_manager API.  No per-file progress feedback.

    Returns:
        dict: blob_name -> raw bytes
    """
    file_objects = [io.BytesIO() for _ in blobs]
    blob_file_pairs = list(zip(blobs, file_objects))

    transfer_manager.download_many(
        blob_file_pairs,
        max_workers=max_workers,
        worker_type='thread',
        raise_exception=True,
    )

    return {
        blob.name: fobj.getvalue()
        for blob, fobj in zip(blobs, file_objects)
    }


def _download_with_progress(blobs, max_workers):
    """
    Experimental path — uses ThreadPoolExecutor + tqdm progress bar.

    Downloads blobs one-by-one in a thread pool.  Each completed download
    updates a tqdm progress bar showing: count, speed, elapsed, and ETA.

    Why not transfer_manager.download_many()?
      download_many() is a single blocking call with no progress callback.
      There is no way to hook into per-file completion for live feedback.

    Returns:
        dict: blob_name -> raw bytes
    """
    downloaded = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download_blob, blob): blob for blob in blobs}

        with tqdm(
            total=len(blobs),
            desc="  Downloading",
            unit="file",
            bar_format="  {l_bar}{bar:30}{r_bar}",
            ncols=90,
        ) as pbar:
            for future in as_completed(futures):
                name, data = future.result()
                downloaded[name] = data
                pbar.update(1)

    return downloaded


def _download_with_gcloud(bucket_name, prefix, limit=None):
    """
    Fast path — uses gcloud storage cp -r command via subprocess.

    This approach matches the professor's solution and avoids API rate limits
    by using gcloud's optimized transfer protocol. Much faster on Cloud Shell
    (4x faster than laptop).

    When limit is set, starts cp -r in background and monitors downloaded files,
    terminating the process once N files are downloaded.

    Args:
        bucket_name (str): GCS bucket name
        prefix (str): Folder prefix within bucket
        limit (int, optional): Maximum number of files to download. If None, download all.

    Returns:
        dict: blob_name -> raw bytes
    """
    # Create temporary directory for downloads
    temp_dir = tempfile.mkdtemp(prefix='gcs_download_')

    try:
        # List blobs via Python API, then batch download with gcloud cp -r
        batch_size = 200

        print_step(f"Listing files in gs://{bucket_name}/{prefix}...")
        client = storage.Client.create_anonymous_client()
        bucket = client.bucket(bucket_name)
        all_uris = [
            f"gs://{bucket_name}/{b.name}"
            for b in bucket.list_blobs(prefix=prefix)
            if b.name.endswith('.html')
        ]
        total = len(all_uris)

        if limit is not None and limit < total:
            all_uris = all_uris[:limit]
            total = limit

        num_batches = (total + batch_size - 1) // batch_size
        print_success(f"Found {total} files — downloading in {num_batches} batches of {batch_size}")
        print()

        total_downloaded = 0
        t_start = time.time()

        for i in range(0, total, batch_size):
            batch = all_uris[i:i + batch_size]
            batch_num = i // batch_size + 1

            subprocess.run(
                ['gcloud', 'storage', 'cp', '-r'] + batch + [temp_dir],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

            total_downloaded += len(batch)
            elapsed = time.time() - t_start
            rate = total_downloaded / elapsed if elapsed > 0 else 0
            remaining = total - total_downloaded
            eta = remaining / rate if rate > 0 else 0
            pct = total_downloaded / total * 100

            print(f"  Batch {batch_num}/{num_batches} | {total_downloaded}/{total} ({pct:.0f}%) | "
                  f"{rate:.0f} files/s | ETA {eta:.0f}s", end='\r')

        elapsed = time.time() - t_start
        print()
        print_success(f"Downloaded {total_downloaded} files in {num_batches} batches ({elapsed:.1f}s)")

        # Walk through temp directory to find downloaded HTML files
        html_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.endswith('.html'):
                    html_files.append(os.path.join(root, file))

        if not limit:
            print_success(f"Processing {len(html_files)} HTML files")

        # Read files into memory
        downloaded = {}
        for filepath in html_files:
            # Extract blob name (relative path from prefix)
            filename = os.path.basename(filepath)
            blob_name = f"{prefix}{filename}"

            with open(filepath, 'rb') as f:
                downloaded[blob_name] = f.read()

        return downloaded

    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)


def read_gcs_files(bucket_name, prefix="generated_htmls/", use_gcloud=True, progress=True, limit=None):
    """
    Read and parse all HTML files from a GCS bucket.

    Args:
        bucket_name (str): Name of the GCS bucket (e.g., 'cs528-hw2-jimmyjia')
        prefix (str): Folder prefix within the bucket (e.g., 'generated_htmls/')
        use_gcloud (bool): If True, use gcloud storage cp (recommended for Cloud Shell).
                          If False, use Python client library with blob downloads.
        progress (bool): If True and use_gcloud=False, use experimental tqdm progress bar.
                         If False and use_gcloud=False, use stable transfer_manager.download_many().
                         Ignored when use_gcloud=True.
        limit (int, optional): Maximum number of files to download. If None, download all files.

    Returns:
        dict: page_id (str) -> list of outgoing link target IDs (list[str])
    """
    print_stage("Read", "Parse HTML files from GCS")

    with Timer("Total Stage 1"):

        # --- Step 1: Download files ---
        if use_gcloud:
            # Fast path: use gcloud storage cp command
            with Timer("Download (gcloud)"):
                downloaded = _download_with_gcloud(bucket_name, prefix, limit=limit)
        else:
            # Original path: use Python client library
            print_step(f"Connecting to bucket: {bucket_name}")
            client = storage.Client()
            bucket = client.bucket(bucket_name)

            with Timer("Listing blobs"):
                blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith('.html')]
            print_success(f"Found {len(blobs)} HTML files")

            # Apply limit if specified
            if limit is not None and limit < len(blobs):
                blobs = blobs[:limit]
                print_success(f"Limiting to {limit} files")

            max_workers = min(32, (os.cpu_count() or 4) + 4)
            mode = "experimental (tqdm)" if progress else "stable (transfer_manager)"
            print_step(f"Downloading {len(blobs)} files with {max_workers} workers [{mode}]...")

            with Timer("Download"):
                if progress:
                    downloaded = _download_with_progress(blobs, max_workers)
                else:
                    downloaded = _download_stable(blobs, max_workers)

        # --- Step 2: Parse each downloaded file to extract outgoing links ---
        print_step("Parsing HTML files for links...")
        with Timer("Parsing"):
            outgoing = {}
            for name, data in downloaded.items():
                page_id = name.split('/')[-1].replace('.html', '')
                outgoing[page_id] = parse_html(data.decode('utf-8'))

        # --- Summary ---
        total_links = sum(len(v) for v in outgoing.values())
        print_summary_box("Stage 1 Summary", {
            "Files parsed": len(outgoing),
            "Total outgoing links": total_links,
            "Avg links/file": f"{total_links / len(outgoing):.1f}",
        })

    return outgoing
