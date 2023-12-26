# SPDX-License-Identifier: MIT
"""Compression utilities."""
from __future__ import annotations

import importlib.util
import zlib
from abc import ABC, abstractmethod

snappy = None
if importlib.util.find_spec("snappy") is not None:
    import snappy

zstd = None
if importlib.util.find_spec("zstd") is not None:
    import zstd


class Compressor(ABC):
    name: str
    """A compressor."""

    @classmethod
    def available(cls) -> bool:
        """Check if the compressor is available.

        Returns:
            bool: True if the compressor is available, False otherwise.
        """
        return True

    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compress the given data.

        Args:
            data (bytes): The data to compress.

        Returns:
            bytes: The compressed data.
        """

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Decompress the given data.

        Args:
            data (bytes): The data to decompress.

        Returns:
            bytes: The decompressed data.
        """


class NoCompression(Compressor):
    name = "noop"
    """No compression."""

    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


class Snappy(Compressor):
    name = "snappy"
    """Snappy compression."""

    @classmethod
    def available(cls) -> bool:
        return snappy is not None

    def __init__(self) -> None:
        if snappy is None:
            err = "Snappy is not installed"
            raise ImportError(err)

        self.snappy = snappy

    def compress(self, data: bytes) -> bytes:
        return self.snappy.compress(data)  # type: ignore

    def decompress(self, data: bytes) -> bytes:
        return self.snappy.decompress(data)  # type: ignore


class Zlib(Compressor):
    name = "zlib"
    """Zlib compression."""

    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class Zstd(Compressor):
    name = "zstd"
    """Zstd compression."""

    @classmethod
    def available(cls) -> bool:
        return zstd is not None

    def __init__(self) -> None:
        if zstd is None:
            err = "Zstd is not installed"
            raise ImportError(err)

        self.zstd = zstd

    def compress(self, data: bytes) -> bytes:
        return self.zstd.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self.zstd.decompress(data)


compressors = [Snappy, Zstd, Zlib, NoCompression]

compression_registry = {
    "snappy": (Snappy, 1),
    "zstd": (Zstd, 3),
    "zlib": (Zlib, 2),
    "noop": (NoCompression, 0),
}

compression_lookup = {c[1]: c[0] for c in compression_registry.values()}


def pick_compressor(available_compressors: list[str]) -> tuple[type[Compressor], int]:
    """Pick a compressor.

    Returns:
        type[Compressor]: The compressor to use.
    """
    # available_compressors is from mongod

    for compressor in available_compressors:
        if compressor in compression_registry:
            return compression_registry[compressor]

    # zlib is always available but type checking doesn't know that
    return NoCompression, 0


def list_compressors() -> list[str]:
    """List the available compressors.

    Returns:
        list[str]: The available compressors.
    """
    return [c.name for c in compressors if c.available()]
