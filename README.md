# CS528 HW2 — PageRank Pipeline

**Author:** Haozhe Jia \<jimmyjia@bu.edu\>\
**Course:** CS528 Cloud Computing, Boston University, Spring 2026\
**Repository:** [github.com — cs528-hw2-pagerank](https://github.com/)\
**GCP Project:** `jimmysproject`\
**GCS Bucket:** `cs528-hw2-jimmyjia` (region: `us-central1`, publicly readable)\

---

## 1. Project Overview

This project implements the **PageRank algorithm** as described by Page, Brin, Motwani & Winograd (1999). The pipeline:

1. Generates 20,000 HTML files with up to 375 links each using a provided Python client.
2. Uploads them to a Google Cloud Storage bucket.
3. Reads and parses all files from GCS in parallel.
4. Computes link statistics (min, max, average, median, quintiles) for incoming and outgoing links.
5. Computes PageRank via power iteration on a sparse stochastic matrix.
6. Validates results against NetworkX's built-in `pagerank()` using Spearman's rho, Kendall's tau, MAE, and Precision@5.

### PageRank Formula

```
PR(A) = (1 - d) / N + d * (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
```

Where `d = 0.85` (damping factor), `N` = total pages, `Ti` = pages linking to `A`, `C(Ti)` = outgoing link count of `Ti`.
Iteration stops when the sum of PageRank changes across all pages is less than **0.5%** (homework threshold), then continues to **N * 1e-6** (NetworkX-level precision) for ranking stability.

---

## 3. GCS Bucket Setup

### 3.1 Create the Bucket

```bash
gcloud storage buckets create gs://cs528-hw2-jimmyjia \
    --location=us-central1 \
```

### 3.2 Generate and Upload HTML Files

```bash
# Generate 20,000 HTML files with max 375 links each
python generate-content.py -n 20000 -m 375

# Upload to GCS
gcloud storage cp *.html gs://cs528-hw2-jimmyjia/generated_htmls/
```

### 3.3 Make Bucket World-Readable

```bash
gcloud storage buckets add-iam-policy-binding gs://cs528-hw2-jimmyjia \
    --member=allUsers \
    --role=roles/storage.objectViewer
```


---

## 4. How to Run

### 4.1 Prerequisites

```bash
# Clone the repository
git clone <repository-url>
cd cs528-hw2-pagerank

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4.2 Run Against GCS Bucket (main pipeline)

```bash
python main.py
```

### 4.3 Run Locally (for testing / development)

```bash
# First generate test files if not already present
python generate-content.py -n 20000 -m 375

# Then run the local pipeline
python test_local.py
```

---

## 5. CLI Parameters

### `main.py`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--bucket` | `cs528-hw2-jimmyjia` | GCS bucket name |
| `--prefix` | `generated_htmls/` | Folder prefix within the bucket |
| `--limit` | `None` (all files) | Limit number of files to download (for quick testing) |
| `--progress` | On (default) | Show live download progress bar with ETA (experimental) |
| `--no-progress` | — | Use stable `transfer_manager.download_many()` without progress bar |

**Examples:**

```bash
# Full run with progress bar
python main.py

# Quick test with 100 files, no progress bar
python main.py --limit 100 --no-progress

# Run against a different bucket
python main.py --bucket another-bucket --prefix html_files/
```

### `test_local.py`

No CLI parameters. Edit the script directly to change the local directory path or file limit.

### `generate-content.py` (provided script)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-n`, `--num_files` | `10000` | Number of HTML files to generate |
| `-m`, `--max_refs` | `250` | Maximum number of links per file |

---


## 6. Output and Timing

### 6.1 Local Machine

```
Environment: [WSL Ubuntu 24.04,AMD Ryzen 7 9700X, 32GB]
Total pipeline time: [X.XX]s
  Stage 1 (read):      280.68 s
  Stage 2 (stats):     0.73 s
  Stage 3 (pagerank):  1.07 s
  Stage 4 (validate):  5.40 s
```

### 6.2 Google Cloud Shell

```
Environment: Cloud Shell (e2-small, Debian)
Total pipeline time: [X.XX]s
  Stage 1 (read):      [X.XX]s
  Stage 2 (stats):     [X.XX]s
  Stage 3 (pagerank):  [X.XX]s
  Stage 4 (validate):  [X.XX]s
```

---

## 7. Cloud Billing

> *Insert screenshot of the GCP Billing Console showing total spend for this project.*

Total spend: $X.XX

---

## 8. References

1. Page, L., Brin, S., Motwani, R., & Winograd, T. (1999). "The PageRank Citation Ranking: Bringing Order to the Web." Stanford InfoLab. http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf

2. Langville, A. & Meyer, C. (2004). "A Survey of Eigenvector Methods of Web Information Retrieval." http://citeseer.ist.psu.edu/713792.html

3. Spearman, C. (1904). "The Proof and Measurement of Association between Two Things." American Journal of Psychology, 15(1), 72-101.

4. Kendall, M. (1938). "A New Measure of Rank Correlation." Biometrika, 30(1/2), 81-93.

5. Kumar, R. & Vassilvitskii, S. (2010). "Generalized Distances between Rankings." WWW 2010. https://theory.stanford.edu/~sergei/papers/www10-metrics.pdf

6. NetworkX 3.6.1 — `_pagerank_scipy` implementation. https://github.com/networkx/networkx/blob/main/networkx/algorithms/link_analysis/pagerank_alg.py (3-clause BSD License)

---

## 9. AI Usage Disclosure

AI assistance was used in this project via **Claude** (Anthropic), accessed through the **Claude Code CLI** tool.

### What AI was used for

- **`utils.py`** — Designing the terminal display interface: ANSI-colored output, bordered summary boxes, side-by-side comparison table rendering, and timing utilities. The prompt was: *"Design a user-friendly terminal interface for the PageRank pipeline that clearly presents each stage's progress, statistics, and results using colored output, bordered summary boxes, side-by-side comparison tables, and timing information."*

- **Code refinement** — Refactoring the iterative PageRank implementation from pure-Python dict loops to scipy sparse matrix operations, referencing NetworkX's `_pagerank_scipy` approach. AI helped identify the sparse matrix pattern and translate the mathematical formulation into vectorized code.

- **Validation metrics** — Identifying and implementing proper ranking comparison metrics (Spearman's rho, Kendall's tau) with academic references.

### What was written by the author

- Core pipeline architecture and stage separation.
- GCS bucket setup, permissions, and data upload.
- Understanding of the PageRank algorithm and convergence criteria.
- All debugging, testing, and verification of correctness.

### Understanding

I understand how each component works:

- **PageRank formula**: The iterative power method applies `x = d * (x @ A + dangling/N) + (1-d)/N` where `A` is a row-normalized sparse adjacency matrix. Each left-multiply `x @ A` computes the weighted sum of incoming PageRank contributions. Dangling nodes (no outgoing links) have their rank redistributed uniformly.

- **Sparse matrix representation**: A CSR (Compressed Sparse Row) matrix stores only non-zero entries, so 20K nodes with ~7.5M edges uses ~60MB instead of the 3.2GB a dense matrix would require.

- **Two-phase convergence**: The 0.5% relative threshold satisfies the homework spec but can leave enough residual error to swap pages with nearly-equal scores. Continuing to N*1e-6 absolute tolerance stabilizes the ranking order to match NetworkX.

- **Validation metrics**: Spearman's rho measures rank-order agreement (penalizes large displacements more), Kendall's tau counts concordant vs discordant pairs (treats all swaps equally). Together they provide a complete picture of ranking similarity.
