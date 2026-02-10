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
#   GCS download strategies (ordered by preference):
#     1. `gcloud storage cp` (CLI) — Google's optimized bulk transfer tool.
#        Uses parallel composite downloads, connection reuse, and C-level I/O.
#        Available on Cloud Shell and any machine with gcloud SDK installed.
#
#     2. Python API fallback — transfer_manager.download_many() or
#        ThreadPoolExecutor + tqdm.  Used when `gcloud` CLI is not available.
#        Slower for large file counts due to per-file HTTP overhead.
#
# References:
#   [1] Google Cloud Storage Python Client — transfer_manager module
#       https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager
#   [2] Downloading objects from GCS
#       https://cloud.google.com/storage/docs/downloading-objects#download-object-python

import re
import os
import io
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_pagerank.utils import print_stage, print_step, print_success, print_warning, print_summary_box, Timer


def parse_html(text):
    """Extract link targets from HTML content."""
    return re.findall(r'<a HREF="(\d+)\.html"', text)


# ===================================================================
# Local reading (shared by local mode and gcloud download)
# ===================================================================

def _read_and_parse(filepath):
    """Read a single HTML file from disk and return (page_id, links)."""
    page_id = os.path.basename(filepath).replace('.html', '')
    with open(filepath, 'r') as f:
        return page_id, parse_html(f.read())


def _read_local(directory, limit=None):
    """
    Read HTML files from a local directory using parallel I/O.

    Args:
        directory (str): Path to directory containing .html files
        limit (int|None): Max number of files to read (None = all)

    Returns:
        dict: page_id -> list of outgoing link target IDs
    """
    print_stage("Read", "Parse HTML files from local directory")

    with Timer("Total Stage 1"):
        files = sorted(f for f in os.listdir(directory) if f.endswith('.html'))
        if limit:
            files = files[:limit]
        filepaths = [os.path.join(directory, f) for f in files]

        max_workers = min(32, (os.cpu_count() or 4) + 4)
        print_step(f"Reading {len(filepaths)} files with {max_workers} threads...")

        with Timer("Read + Parse"):
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                outgoing = dict(pool.map(_read_and_parse, filepaths))

        total_links = sum(len(v) for v in outgoing.values())
        print_summary_box("Stage 1 Summary", {
            "Source": directory,
            "Files parsed": len(outgoing),
            "Total outgoing links": total_links,
            "Avg links/file": f"{total_links / len(outgoing):.1f}",
        })

    return outgoing


# ===================================================================
# GCS reading
# ===================================================================

def _download_via_gcloud(bucket_name, prefix, tmp_dir):
    """
    Download all HTML files from GCS using `gcloud storage cp`.

    This is the fastest path — gcloud CLI uses parallel composite downloads
    and optimized connection handling.  Available on Cloud Shell by default.

    Returns:
        True if successful, False if gcloud is not available.
    """
    gcs_url = f"gs://{bucket_name}/{prefix}*.html"
    cmd = ["gcloud", "storage", "cp", gcs_url, tmp_dir]
    print_step(f"Downloading via: gcloud storage cp {gcs_url} ...")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode == 0:
            return True
        else:
            print_warning(f"gcloud cp failed: {result.stderr.strip()[:200]}")
            return False
    except FileNotFoundError:
        print_warning("gcloud CLI not found, falling back to Python API")
        return False
    except subprocess.TimeoutExpired:
        print_warning("gcloud cp timed out after 600s")
        return False


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
    from google.cloud.storage import transfer_manager

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

    Returns:
        dict: blob_name -> raw bytes
    """
    from tqdm import tqdm

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


def _read_gcs(bucket_name, prefix="generated_htmls/", limit=None, progress=False):
    """
    Read and parse all HTML files from a GCS bucket.

    Download strategy (in order of preference):
      1. `gcloud storage cp` — fastest, uses Google's optimized CLI.
      2. Python API fallback — transfer_manager or tqdm ThreadPool.

    Args:
        bucket_name (str): Name of the GCS bucket
        prefix (str): Folder prefix within the bucket
        limit (int|None): Max number of files to download (None = all)
        progress (bool): If True, use tqdm progress bar (Python API only).

    Returns:
        dict: page_id -> list of outgoing link target IDs
    """
    print_stage("Read", "Parse HTML files from GCS")

    with Timer("Total Stage 1"):

        # --- Strategy 1: Try gcloud CLI (fast bulk download) ---
        tmp_dir = tempfile.mkdtemp(prefix="pagerank_")
        try:
            with Timer("Download"):
                gcloud_ok = _download_via_gcloud(bucket_name, prefix, tmp_dir)

            if gcloud_ok:
                print_success("Downloaded via gcloud CLI")

                files = sorted(f for f in os.listdir(tmp_dir) if f.endswith('.html'))
                if limit:
                    files = files[:limit]
                filepaths = [os.path.join(tmp_dir, f) for f in files]
                print_success(f"Found {len(filepaths)} HTML files")

                print_step("Parsing HTML files for links...")
                max_workers = min(32, (os.cpu_count() or 4) + 4)
                with Timer("Parsing"):
                    with ThreadPoolExecutor(max_workers=max_workers) as pool:
                        outgoing = dict(pool.map(_read_and_parse, filepaths))

                total_links = sum(len(v) for v in outgoing.values())
                print_summary_box("Stage 1 Summary", {
                    "Source": f"gs://{bucket_name}/{prefix}",
                    "Method": "gcloud storage cp",
                    "Files parsed": len(outgoing),
                    "Total outgoing links": total_links,
                    "Avg links/file": f"{total_links / len(outgoing):.1f}",
                })
                return outgoing

            # --- Strategy 2: Fall back to Python API ---
            print_step("Falling back to Python API...")
            from google.cloud import storage

            print_step(f"Connecting to bucket: {bucket_name}")
            try:
                client = storage.Client()
                print_success("Authenticated client (internal network)")
            except Exception:
                client = storage.Client.create_anonymous_client()
                print_success("Anonymous client (public endpoint)")
            bucket = client.bucket(bucket_name)

            with Timer("Listing blobs"):
                blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith('.html')]
            if limit:
                blobs = blobs[:limit]
            print_success(f"Found {len(blobs)} HTML files")

            max_workers = 16
            print_step(f"Downloading {len(blobs)} files with {max_workers} workers...")

            with Timer("Download"):
                if progress:
                    downloaded = _download_with_progress(blobs, max_workers)
                else:
                    downloaded = _download_stable(blobs, max_workers)

            print_step("Parsing HTML files for links...")
            with Timer("Parsing"):
                outgoing = {}
                for name, data in downloaded.items():
                    page_id = name.split('/')[-1].replace('.html', '')
                    outgoing[page_id] = parse_html(data.decode('utf-8'))

            total_links = sum(len(v) for v in outgoing.values())
            print_summary_box("Stage 1 Summary", {
                "Source": f"gs://{bucket_name}/{prefix}",
                "Method": "Python API (transfer_manager)" if not progress else "Python API (tqdm)",
                "Files parsed": len(outgoing),
                "Total outgoing links": total_links,
                "Avg links/file": f"{total_links / len(outgoing):.1f}",
            })

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return outgoing


# ===================================================================
# Unified entry point — auto-detects GCS vs local
# ===================================================================

def read_files(source, prefix="generated_htmls/", limit=None, progress=False):
    """
    Read and parse HTML files. Auto-detects source type:
      - If `source` is an existing local directory → read from disk.
      - Otherwise → treat `source` as a GCS bucket name.

    Args:
        source (str): Local directory path or GCS bucket name
        prefix (str): GCS folder prefix (ignored for local)
        limit (int|None): Max number of files to read
        progress (bool): Show tqdm progress bar for GCS downloads (Python API only)

    Returns:
        dict: page_id (str) -> list of outgoing link target IDs
    """
    if os.path.isdir(source):
        print_step(f"Detected local directory: {source}")
        return _read_local(source, limit=limit)
    else:
        print_step(f"Detected GCS bucket: {source}")
        return _read_gcs(source, prefix=prefix, limit=limit, progress=progress)
