"""
Minimal imghdr shim for Python 3.13+ environments where the stdlib module was removed.
Implements only what() used by libraries to detect common image types.
"""

from typing import Optional, Union


def _detect_from_header(header: bytes) -> Optional[str]:
    if len(header) < 12:
        return None

    # JPEG: FF D8 FF
    if header[:3] == b"\xff\xd8\xff":
        return "jpeg"

    # PNG: 89 50 4E 47 0D 0A 1A 0A
    if header[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"

    # GIF87a / GIF89a
    if header[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"

    # WEBP: RIFF....WEBP
    if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "webp"

    # BMP: 'BM'
    if header[:2] == b"BM":
        return "bmp"

    # TIFF: II*\x00 or MM\x00*
    if header[:4] in (b"II*\x00", b"MM\x00*"):
        return "tiff"

    return None


def what(file: Union[str, bytes, int], h: Optional[bytes] = None) -> Optional[str]:
    """
    Roughly compatible with the old imghdr.what API. Accepts a file path or a file-like
    descriptor; if header bytes are provided via h, they take precedence.
    """
    if h is not None:
        return _detect_from_header(h)

    try:
        # file may be a path or a file descriptor int
        if isinstance(file, int):
            import os
            with os.fdopen(file, "rb", closefd=False) as f:  # do not close caller's fd
                header = f.read(32)
        else:
            with open(file, "rb") as f:
                header = f.read(32)
        return _detect_from_header(header)
    except Exception:
        return None


