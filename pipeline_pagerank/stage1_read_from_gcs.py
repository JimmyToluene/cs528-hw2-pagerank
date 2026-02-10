# stage1_read_from_gcs.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Stage 1 — Read and parse HTML files from a Google Cloud Storage bucket.
#   Lists all blobs first, then downloads them using one of several strategies,
#   each with a tqdm progress bar.
#
#   Download strategies (selectable via `method` parameter):
#
#     "thread_pool" (default):
#       ThreadPoolExecutor + blob.download_to_file() with per-file tqdm updates.
#
#     "transfer_manager":
#       google.cloud.storage.transfer_manager.download_many() — bulk parallel
#       download with a single tqdm spinner (no per-file granularity since the
#       API is a single blocking call).
#
#     "sequential":
#       Simple sequential blob.download_as_bytes() loop with tqdm — useful as
#       a baseline or when thread overhead is undesirable.
#
#     "gcloud":
#       Shells out to `gcloud storage cp` in batches — fastest on Cloud Shell
#       where gcloud uses an optimized transfer protocol.
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from google.cloud.storage import transfer_manager
from tqdm import tqdm
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_summary_box, Timer


def parse_html(text):
    """Extract link targets from HTML content."""
    return re.findall(r'<a HREF="(\d+)\.html"', text)


# ---------------------------------------------------------------------------
#  Download strategies — each returns dict[blob_name, bytes]
# ---------------------------------------------------------------------------

def _download_thread_pool(blobs, max_workers):
    """
    ThreadPoolExecutor + tqdm progress bar.

    Each blob is downloaded individually in a thread pool. Every completed
    download ticks the tqdm bar, giving real-time progress with ETA.
    """
    downloaded = {}

    def _fetch(blob):
        buf = io.BytesIO()
        blob.download_to_file(buf)
        return blob.name, buf.getvalue()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch, blob): blob for blob in blobs}

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


def _download_transfer_manager(blobs, max_workers):
    """
    transfer_manager.download_many() with tqdm wrapper.

    download_many() is a single blocking call with no per-file callback.
    We show an indeterminate tqdm spinner while it runs, then jump to 100%.
    """
    file_objects = [io.BytesIO() for _ in blobs]
    blob_file_pairs = list(zip(blobs, file_objects))

    with tqdm(
        total=len(blobs),
        desc="  Downloading",
        unit="file",
        bar_format="  {l_bar}{bar:30}{r_bar}",
        ncols=90,
    ) as pbar:
        transfer_manager.download_many(
            blob_file_pairs,
            max_workers=max_workers,
            worker_type='thread',
            raise_exception=True,
        )
        pbar.update(len(blobs))

    return {
        blob.name: fobj.getvalue()
        for blob, fobj in zip(blobs, file_objects)
    }


def _download_sequential(blobs):
    """
    Simple sequential download with tqdm.

    Downloads blobs one at a time — no threading. Useful as a baseline
    or for debugging. tqdm updates after every file.
    """
    downloaded = {}
    with tqdm(
        total=len(blobs),
        desc="  Downloading",
        unit="file",
        bar_format="  {l_bar}{bar:30}{r_bar}",
        ncols=90,
    ) as pbar:
        for blob in blobs:
            data = blob.download_as_bytes()
            downloaded[blob.name] = data
            pbar.update(1)

    return downloaded


def _download_gcloud(blobs, bucket_name, prefix):
    """
    gcloud storage cp in batches with tqdm.

    Shells out to `gcloud storage cp` for each batch of URIs.
    Fastest on Cloud Shell where gcloud uses an optimized transfer protocol.
    tqdm updates after each batch completes.
    """
    temp_dir = tempfile.mkdtemp(prefix='gcs_download_')
    batch_size = 200

    try:
        all_uris = [f"gs://{bucket_name}/{b.name}" for b in blobs]
        total = len(all_uris)
        num_batches = (total + batch_size - 1) // batch_size

        with tqdm(
            total=total,
            desc="  Downloading",
            unit="file",
            bar_format="  {l_bar}{bar:30}{r_bar}",
            ncols=90,
        ) as pbar:
            for i in range(0, total, batch_size):
                batch = all_uris[i:i + batch_size]

                result = subprocess.run(
                    ['gcloud', 'storage', 'cp', '-r'] + batch + [temp_dir],
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                )
                if result.returncode != 0:
                    batch_num = i // batch_size + 1
                    print(f"\n  [WARN] Batch {batch_num} error: {result.stderr.strip()}")

                pbar.update(len(batch))

        # Read downloaded files back into memory
        downloaded = {}
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.endswith('.html'):
                    filepath = os.path.join(root, file)
                    blob_name = f"{prefix}{file}"
                    with open(filepath, 'rb') as f:
                        downloaded[blob_name] = f.read()

        return downloaded

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
#  Main entry point
# ---------------------------------------------------------------------------

def read_gcs_files(bucket_name, prefix="generated_htmls/", method="thread_pool", limit=None):
    """
    Read and parse all HTML files from a GCS bucket.

    Step 1: List all blobs under `prefix`.
    Step 2: Download them using the chosen method (all with tqdm progress).
    Step 3: Parse HTML to extract outgoing links.

    Args:
        bucket_name (str): Name of the GCS bucket (e.g., 'cs528-hw2-jimmyjia')
        prefix (str): Folder prefix within the bucket (e.g., 'generated_htmls/')
        method (str): Download strategy — one of:
            "thread_pool"       — ThreadPoolExecutor + per-file tqdm (default)
            "transfer_manager"  — transfer_manager.download_many() + tqdm
            "sequential"        — one-by-one download + tqdm
            "gcloud"            — gcloud storage cp in batches + tqdm
        limit (int, optional): Maximum number of files to download.

    Returns:
        dict: page_id (str) -> list of outgoing link target IDs (list[str])
    """
    print_stage("Read", "Parse HTML files from GCS")

    with Timer("Total Stage 1"):

        # --- Step 1: List blobs ---
        print_step(f"Connecting to bucket: {bucket_name}")
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        with Timer("Listing blobs"):
            blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith('.html')]
        print_success(f"Found {len(blobs)} HTML files")

        if limit is not None and limit < len(blobs):
            blobs = blobs[:limit]
            print_success(f"Limiting to {limit} files")

        # --- Step 2: Download ---
        max_workers = min(32, (os.cpu_count() or 4) + 4)
        print_step(f"Downloading {len(blobs)} files [{method}] ...")

        with Timer("Download"):
            if method == "thread_pool":
                downloaded = _download_thread_pool(blobs, max_workers)
            elif method == "transfer_manager":
                downloaded = _download_transfer_manager(blobs, max_workers)
            elif method == "sequential":
                downloaded = _download_sequential(blobs)
            elif method == "gcloud":
                downloaded = _download_gcloud(blobs, bucket_name, prefix)
            else:
                raise ValueError(f"Unknown download method: {method!r}")

        # --- Step 3: Parse HTML ---
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
