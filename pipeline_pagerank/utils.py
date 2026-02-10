# utils.py
#
# Project: CS528 HW2 — PageRank Pipeline
# Author:  Haozhe Jia <jimmyjia@bu.edu>
# Course:  CS528 Cloud Computing, Boston University, Spring 2026
#
# Description:
#   Terminal display utilities — colored output, summary boxes,
#   side-by-side table rendering, and timing context manager.
#
# AI Assistance Disclosure:
#   This file was developed with assistance from Claude (Anthropic),
#   accessed via the Claude Code CLI tool.
#
#   Prompt / purpose:
#     "Design a user-friendly terminal interface for the PageRank pipeline
#      that clearly presents each stage's progress, statistics, and results
#      using colored output, bordered summary boxes, side-by-side comparison
#      tables, and timing information."
#
#   Why AI was used:
#     The core logic of this project (PageRank algorithm, GCS integration,
#     statistics) was written by the author.  AI was used specifically for
#     the display layer — designing a clean, readable terminal UI with
#     ANSI color codes, box-drawing layouts, and side-by-side rendering.
#     These are presentational concerns that benefit from rapid prototyping
#     but do not affect the correctness of the pipeline's computations.
#
# Components:
#   Colors            — ANSI escape code constants for terminal styling.
#   print_project_banner — Project metadata banner (author, course, ref).
#   print_stage / print_step / print_success / print_warning / print_error
#                     — Hierarchical log output with color-coded prefixes.
#   print_summary_box — Single bordered table for key-value statistics.
#   print_side_by_side_boxes
#                     — Two bordered tables rendered on the same lines
#                       (e.g., [Outgoing Stats] [Incoming Stats]).
#   print_dict_sanity_check
#                     — Quick preview of a page_id -> links dictionary.
#   Timer             — Context manager that prints elapsed wall time.

import time


class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    RESET = '\033[0m'


def print_project_banner():
    """Print project info banner at pipeline start."""
    w = 90
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * w}")
    print(f"  CS528 HW2 — PageRank Pipeline")
    print(f"{'=' * w}{Colors.RESET}")
    print(f"  {Colors.DIM}Author:{Colors.RESET}  Haozhe Jia <jimmyjia@bu.edu>")
    print(f"  {Colors.DIM}Course:{Colors.RESET}  CS528 Cloud Computing, Boston University")
    print(f"  {Colors.DIM}Ref:{Colors.RESET}     Page, Brin, Motwani & Winograd (1999)")
    print(f"           {Colors.DIM}\"The PageRank Citation Ranking\"")
    print(f"           http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * w}{Colors.RESET}\n")


def print_stage(name, message):
    """Print a stage header."""
    print(f"{Colors.BOLD}{Colors.CYAN}[{name}]{Colors.RESET} {message}")


def print_step(message):
    """Print a sub-step within a stage."""
    print(f"  {Colors.DIM}->{Colors.RESET} {message}")


def print_success(message):
    """Print a success message."""
    print(f"  {Colors.GREEN}[OK]{Colors.RESET} {message}")


def print_warning(message):
    """Print a warning message."""
    print(f"  {Colors.YELLOW}[WARN]{Colors.RESET} {message}")


def print_error(message):
    """Print an error message."""
    print(f"  {Colors.RED}[ERR]{Colors.RESET} {message}")


def print_stat(label, value):
    """Print a statistic line."""
    print(f"  | {label}: {Colors.BOLD}{value}{Colors.RESET}")


def print_summary_box(title, stats):
    """
    Print a single summary box.

    Args:
        title (str): Box title
        stats (dict): Key-value pairs to display
    """
    width = 50
    print(f"\n  +{'-' * width}+")
    padded = title + ' ' * (width - 1 - len(title))
    print(f"  | {Colors.BOLD}{padded}{Colors.RESET}|")
    print(f"  +{'-' * width}+")
    for key, val in stats.items():
        line = f" {key}: {val}"
        print(f"  |{line:<{width}}|")
    print(f"  +{'-' * width}+\n")


def _build_box_lines(title, stats, width):
    """Build a box as a list of strings for side-by-side rendering."""
    lines = []
    sep = f"+{'-' * width}+"
    lines.append(sep)
    padded = title + ' ' * (width - 1 - len(title))
    lines.append(f"| {Colors.BOLD}{padded}{Colors.RESET}|")
    lines.append(sep)
    for key, val in stats.items():
        content = f" {key}: {val}"
        lines.append(f"|{content:<{width}}|")
    lines.append(sep)
    return lines


def print_side_by_side_boxes(title_l, stats_l, title_r, stats_r, col_width=38, gap=3):
    """
    Print two summary boxes side by side.

    Args:
        title_l (str): Left box title
        stats_l (dict): Left box key-value pairs
        title_r (str): Right box title
        stats_r (dict): Right box key-value pairs
        col_width (int): Inner width of each box
        gap (int): Space between the two boxes
    """
    left = _build_box_lines(title_l, stats_l, col_width)
    right = _build_box_lines(title_r, stats_r, col_width)

    # Pad shorter side so both have equal line count
    empty = ' ' * (col_width + 2)
    max_len = max(len(left), len(right))
    left += [empty] * (max_len - len(left))
    right += [empty] * (max_len - len(right))

    spacer = ' ' * gap
    print()
    for l, r in zip(left, right):
        print(f"  {l}{spacer}{r}")
    print()

def print_dict_sanity_check(data, label="Data", num_preview=5):
    """
    Print sanity check info for a dictionary of page_id -> list mappings.
    Works for both outgoing and incoming link dictionaries.

    Args:
        data (dict): page_id (str) -> list of link targets/sources
        label (str): Label for display (e.g., "Outgoing", "Incoming")
        num_preview (int): Number of pages to preview
    """
    total_links = sum(len(v) for v in data.values())
    zero_links = sum(1 for v in data.values() if len(v) == 0)

    print_summary_box(f"{label} Sanity Check", {
        "Total files": len(data),
        "Total links": total_links,
        "Avg links/file": f"{total_links / len(data):.1f}" if data else "N/A",
        "Files with zero links": zero_links,
    })

    print_step(f"First {num_preview} pages (sorted by ID):")
    for page_id in sorted(data.keys(), key=lambda x: int(x))[:num_preview]:
        preview = data[page_id][:5]
        print(f"      Page {page_id}: {len(data[page_id])} links -> {preview}...")
    print()

class Timer:
    """Context manager for timing code blocks."""
    def __init__(self, label="Operation"):
        self.label = label
        self.start = None
        self.elapsed = None

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self.start
        print_success(f"{self.label} completed in {self.elapsed:.2f}s")