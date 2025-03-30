import json
from pathlib import Path

# Get the project root dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[0]  # Moves up to `root-dir`


def load_json(path: str):
    """Load an JSON file from the path."""
    with open(PROJECT_ROOT / f"{path}", "r") as f:
        return json.load(f)
