import tarfile
import os


DEFAULT_EXCLUDE_DIRS = [".git", "__pycache__"]

def create_tar_archive(source_dir, output_filename, excluded_dirs=DEFAULT_EXCLUDE_DIRS):
    with tarfile.open(output_filename, "w:gz") as tar:
        for root, dirs, files in os.walk(source_dir):
            dirs[:] = [d for d in dirs if d not in excluded_dirs]
            for file in files:
                file_path = os.path.join(root, file)
                tar.add(file_path, arcname=os.path.relpath(file_path, source_dir))


