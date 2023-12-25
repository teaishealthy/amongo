# SPDX-License-Identifier: MIT

"""Errors raised by amongo."""


class DatabaseError(Exception):
    """Exception raised when an error occurs with the database."""


class NotReadyError(Exception):
    """Exception raised when the database is not ready."""

    msg = "Connection not established. Did you forget to call `open()`?"


class CursorIsEmptyError(Exception):
    """Raised when a cursor is empty."""

    msg = "Cursor is empty."
