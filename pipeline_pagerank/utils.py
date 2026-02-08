# pipeline_pagerank/utils.py

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


def print_banner(title):
    """Print a styled banner for major sections."""
    width = 60
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}{Colors.RESET}\n")


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
    Print a summary box with multiple stats.

    Args:
        title (str): Box title
        stats (dict): Key-value pairs to display
    """
    width = 50
    print(f"\n  +{'-' * width}+")
    print(f"  | {Colors.BOLD}{title:<{width - 1}}{Colors.RESET}|")
    print(f"  +{'-' * width}+")
    for key, val in stats.items():
        line = f" {key}: {val}"
        print(f"  |{line:<{width}}|")
    print(f"  +{'-' * width}+\n")


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