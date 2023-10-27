import os
from pathlib import Path
from typing import Tuple


def get_abs_rel_paths(input_path: Path, base_path: Path) -> Tuple[Path, Path]:
    """
    Get the absolute and relative paths of input file path.

    Args:
        input_path: An absolute or relative path.
        base_path: base directory the input is relative to.

    Returns:
        The absolute and relative paths of the file.
    """
    if os.path.isabs(input_path):
        abs_path = input_path
        rel_path = input_path.relative_to(base_path)
    else:
        rel_path = input_path
        abs_path = base_path / rel_path

    return abs_path, rel_path
