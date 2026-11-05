"""Checks every uploaded ZIP goes through before a member is read: escaping paths, symlinks, encryption, bombs."""
import zipfile
from pathlib import PurePosixPath

MAX_COMPRESSION_RATIO = 200


def is_unsafe_zip_member(info: zipfile.ZipInfo) -> bool:
    """A member whose name could land outside the extraction root, or that is a symlink."""
    name = info.filename
    if not name or name.startswith('/') or '\\' in name or '\x00' in name:
        return True
    if '..' in PurePosixPath(name).parts or ':' in name.split('/', 1)[0]:
        return True
    is_symlink = (info.external_attr >> 16) & 0o170000 == 0o120000
    return is_symlink


def is_encrypted_zip_member(info: zipfile.ZipInfo) -> bool:
    return bool(info.flag_bits & 0x1)


def is_compression_bomb(info: zipfile.ZipInfo) -> bool:
    """A member over 1 MB that expands more than ``MAX_COMPRESSION_RATIO`` times its stored size."""
    return info.file_size > 1024 * 1024 and info.file_size > max(info.compress_size, 1) * MAX_COMPRESSION_RATIO
