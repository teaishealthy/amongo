"""Stubs for the bson module."""
from typing import Any

def loads(data: bytes) -> Any:
    """Decode BSON data.

    Args:
        data (bytes): The BSON data to decode.

    Returns:
        Any: The decoded data.
    """

def dumps(data: Any) -> bytes:
    """Encode data as BSON.

    Args:
        data (Any): The data to encode.

    Returns:
        bytes: The encoded data.
    """
