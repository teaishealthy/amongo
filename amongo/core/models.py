# SPDX-License-Identifier: MIT
from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag
from typing import NamedTuple, TypeVar

T = TypeVar("T", bound="Flags")


class Flags(IntFlag):
    """Flags for the OP_MSG Opcode."""

    checksum_present = 1 << 0
    more_to_come = 1 << 1
    exhaust_allowed = 1 << 16
    all = checksum_present | more_to_come | exhaust_allowed

    def verify(self: T) -> T:
        """Verify that only known flags are set.

        Args:
            self (T): The flags to verify.

        Raises:
            ValueError: If an unknown flag is set.

        Returns:
            T: The flags. Useful for chaining and inline usage.
        """
        # The first 16 bits (0-15) are required
        # and parsers MUST error if an unknown bit is set.
        if self & ~Flags.all:
            msg = "Unknown bit set in flags"
            raise ValueError(msg)
        return self


class MessageHeader(NamedTuple):
    message_length: int
    request_id: int
    response_to: int
    opcode: int


class WireItem(NamedTuple):
    header: MessageHeader
    data: bytes


@dataclass
class ConnectionOptions:
    compressors: list[str] | None = None
