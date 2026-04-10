#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" File utilities for filename sanitization and handling """

import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Tuple

from pylon.core.tools import log

# Base directory for chunk uploads
CHUNKS_TEMP_DIR = os.path.join(tempfile.gettempdir(), 'elitea_chunks')


def sanitize_filename(filename: str, existing_names: Optional[List[str]] = None) -> Tuple[str, bool]:
    """
    Sanitize a filename to be safe for filesystem storage and regex pattern matching.
    
    Uses a whitelist approach allowing only alphanumeric characters, underscores,
    hyphens, dots, and Unicode letters/digits. Preserves file extensions.
    
    Args:
        filename: The original filename to sanitize
        existing_names: Optional list of existing filenames to avoid collisions
        
    Returns:
        Tuple of (sanitized_filename, was_modified) where:
        - sanitized_filename: The safe filename
        - was_modified: Boolean indicating if the filename was changed
        
    Examples:
        >>> sanitize_filename("[Test].pdf")
        ('Test.pdf', True)
        
        >>> sanitize_filename("My Document (v2).docx")
        ('My-Document-v2.docx', True)
        
        >>> sanitize_filename("file.txt", ["file.txt", "file_1.txt"])
        ('file_2.txt', True)
    """
    if not filename or not filename.strip():
        return "unnamed_file", True
    
    original_filename = filename
    
    # Split into name and extension
    path_obj = Path(filename)
    name = path_obj.stem
    extension = path_obj.suffix
    
    # Sanitize the name part (preserve Unicode letters/digits)
    # Remove or replace problematic characters
    # Keep: alphanumeric (including Unicode), underscore, hyphen, space
    sanitized_name = re.sub(r'[^\w\s-]', '', name, flags=re.UNICODE)
    
    # Replace multiple spaces/hyphens with single hyphen
    sanitized_name = re.sub(r'[-\s]+', '-', sanitized_name)
    
    # Remove leading/trailing hyphens and spaces
    sanitized_name = sanitized_name.strip('-').strip()
    
    # If name is empty after sanitization, use a default
    if not sanitized_name:
        sanitized_name = "file"
    
    # Sanitize extension (should already be safe, but ensure it starts with dot)
    if extension and not extension.startswith('.'):
        extension = '.' + extension
    
    # Remove problematic characters from extension
    if extension:
        extension = re.sub(r'[^\w.-]', '', extension, flags=re.UNICODE)
    
    # Reconstruct filename
    sanitized_filename = sanitized_name + extension
    
    # Handle collisions with existing names
    if existing_names and sanitized_filename in existing_names:
        counter = 1
        base_name = sanitized_name
        while True:
            sanitized_filename = f"{base_name}_{counter}{extension}"
            if sanitized_filename not in existing_names:
                break
            counter += 1
    
    # Check if filename was modified
    was_modified = (sanitized_filename != original_filename)
    
    return sanitized_filename, was_modified


def create_safe_filename_from_title(title: str) -> str:
    """
    Create a safe filename from a title string.
    
    This is a simpler version for creating filenames from titles/descriptions
    rather than sanitizing existing filenames.
    
    Args:
        title: The title to convert to a filename
        
    Returns:
        A safe filename (without extension)
        
    Example:
        >>> create_safe_filename_from_title("My Project: Phase 1")
        'my-project-phase-1'
    """
    # Remove non-word characters except spaces and hyphens
    safe_name = re.sub(r'[^\w\s-]', '', title, flags=re.UNICODE)
    
    # Replace spaces and multiple hyphens with single hyphen
    safe_name = re.sub(r'[-\s]+', '-', safe_name)
    
    # Convert to lowercase and strip hyphens
    safe_name = safe_name.strip('-').lower()
    
    # Return default if empty
    return safe_name if safe_name else 'untitled'


def get_chunk_dir(file_id: str) -> Path:
    """
    Get the directory path for storing chunks of a specific file upload.

    Args:
        file_id: Unique identifier for the file upload session

    Returns:
        Path object for the chunk directory
    """
    return Path(CHUNKS_TEMP_DIR) / file_id


def save_chunk(file_id: str, chunk_index: int, chunk_data: BinaryIO) -> Path:
    """
    Save a chunk to the temporary directory.

    Args:
        file_id: Unique identifier for the file upload session
        chunk_index: Index of the current chunk (0-based)
        chunk_data: Binary file-like object containing chunk data

    Returns:
        Path to the saved chunk file
    """
    chunk_dir = get_chunk_dir(file_id)
    chunk_dir.mkdir(parents=True, exist_ok=True)

    chunk_path = chunk_dir / f"chunk_{chunk_index:06d}"
    with open(chunk_path, 'wb') as f:
        shutil.copyfileobj(chunk_data, f)

    return chunk_path


def get_received_chunk_count(file_id: str) -> int:
    """
    Get the number of chunks received for a file upload.

    Args:
        file_id: Unique identifier for the file upload session

    Returns:
        Number of chunk files in the directory
    """
    chunk_dir = get_chunk_dir(file_id)
    if not chunk_dir.exists():
        return 0
    return len(list(chunk_dir.glob("chunk_*")))


def are_all_chunks_received(file_id: str, total_chunks: int) -> bool:
    """
    Check if all chunks have been received for a file upload.

    Args:
        file_id: Unique identifier for the file upload session
        total_chunks: Expected total number of chunks

    Returns:
        True if all chunks are present, False otherwise
    """
    return get_received_chunk_count(file_id) >= total_chunks


def merge_chunks(file_id: str, total_chunks: int, output_path: Path) -> int:
    """
    Merge all chunks into a single file.

    Args:
        file_id: Unique identifier for the file upload session
        total_chunks: Total number of chunks to merge
        output_path: Path where the merged file should be written

    Returns:
        Total size of the merged file in bytes

    Raises:
        FileNotFoundError: If any chunk is missing
    """
    chunk_dir = get_chunk_dir(file_id)
    total_size = 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'wb') as output_file:
        for i in range(total_chunks):
            chunk_path = chunk_dir / f"chunk_{i:06d}"
            if not chunk_path.exists():
                raise FileNotFoundError(f"Chunk {i} not found for file_id {file_id}")

            with open(chunk_path, 'rb') as chunk_file:
                shutil.copyfileobj(chunk_file, output_file)
                total_size += chunk_path.stat().st_size

    return total_size


def cleanup_chunks(file_id: str) -> None:
    """
    Remove all chunks and the chunk directory for a file upload.

    Args:
        file_id: Unique identifier for the file upload session
    """
    chunk_dir = get_chunk_dir(file_id)
    if chunk_dir.exists():
        shutil.rmtree(chunk_dir, ignore_errors=True)


def cleanup_stale_chunks(max_age_seconds: int = 43200) -> Dict[str, int]:
    """
    Clean up stale chunk directories that are older than the specified age.

    This function scans the CHUNKS_TEMP_DIR for orphaned chunk directories
    from incomplete or failed uploads and removes them if they exceed
    the maximum age threshold.

    Args:
        max_age_seconds: Maximum age in seconds before a chunk directory
                         is considered stale and eligible for cleanup.
                         Default is 43,200 (12 hours).
    Returns:
        Dict with cleanup statistics:
        - 'directories_scanned': Total number of directories checked
        - 'directories_removed': Number of stale directories removed
        - 'errors': Number of errors encountered during cleanup
    """
    stats = {
        'directories_scanned': 0,
        'directories_removed': 0,
        'errors': 0
    }

    chunks_base_dir = Path(CHUNKS_TEMP_DIR)

    if not chunks_base_dir.exists():
        log.debug("Chunks temp directory does not exist: %s", CHUNKS_TEMP_DIR)
        return stats

    current_time = time.time()

    try:
        for chunk_dir in chunks_base_dir.iterdir():
            if not chunk_dir.is_dir():
                continue
            stats['directories_scanned'] += 1
            try:
                # Get the modification time of the directory
                dir_mtime = chunk_dir.stat().st_mtime
                age_seconds = current_time - dir_mtime

                if age_seconds > max_age_seconds:
                    shutil.rmtree(chunk_dir, ignore_errors=True)
                    stats['directories_removed'] += 1

            except OSError as e:
                log.warning(
                    "Error processing chunk directory %s: %s",
                    chunk_dir.name,
                    str(e)
                )
                stats['errors'] += 1

    except OSError as e:
        log.error("Error scanning chunks temp directory: %s", str(e))
        stats['errors'] += 1

    if stats['directories_removed'] > 0:
        log.info(
            "Stale chunks cleanup completed: scanned=%d, removed=%d, errors=%d",
            stats['directories_scanned'],
            stats['directories_removed'],
            stats['errors']
        )
    else:
        log.debug(
            "Stale chunks cleanup completed: scanned=%d, removed=%d, errors=%d",
            stats['directories_scanned'],
            stats['directories_removed'],
            stats['errors']
        )

    return stats
