# SPDX-License-Identifier: MIT

"""Errors raised by amongo."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .typings import Document


class DatabaseError(Exception):
    """Exception raised when an error occurs with the database."""

    def __init__(self, error: Document) -> None:
        """Create a new DatabaseError instance.

        Args:
            error (Document): The error document.
        """
        self.error = error


class NotReadyError(Exception):
    """Exception raised when the database is not ready."""

    msg = "Connection not established. Did you forget to call `open()`?"


class CursorIsEmptyError(Exception):
    """Raised when a cursor is empty."""

    msg = "Cursor is empty."
