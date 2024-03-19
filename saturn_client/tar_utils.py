import tarfile
import os
from fnmatch import fnmatch
from typing import List

DEFAULT_EXCLUDE_GLOBS = [".git", "__pycache__"]


def check_exclude_globs(input_string, exclude_globs) -> bool:
    """
    returns True if input_string matches a list of globs that we want to exclude
    """
    for glob in exclude_globs:
        if fnmatch(input_string, glob):
            return True
    return False


def create_tar_archive(
    source_dir: str, output_filename: str, exclude_globs: List[str] = DEFAULT_EXCLUDE_GLOBS
) -> None:
    with tarfile.open(output_filename, "w:gz") as tar:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = str(os.path.join(root, file))
                if check_exclude_globs(file_path, exclude_globs):
                    continue
                print(f"adding {file_path}")
                tar.add(file_path, arcname=str(os.path.relpath(file_path, source_dir)))
