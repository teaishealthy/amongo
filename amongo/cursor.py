# SPDX-License-Identifier: MIT

"""A MongoDB cursor."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from .connection import Connection


class _CursorType(TypedDict):
    id: int
    nextBatch: list[dict[str, Any]]
    ns: str


class CursorIsEmptyError(Exception):
    """Raised when a cursor is empty."""


class Cursor:
    """A MongoDB cursor."""

    def __init__(self, connection: Connection, result: Any) -> None:
        """Create a new Cursor instance. This should not be called directly.

        Args:
            connection (Connection): The connection to use.
            result (FindManyResult): The result of the find_many operation.
        """
        self._connection = connection
        self._cursor: _CursorType = {
            "id": result["cursor"]["id"],
            "nextBatch": result["cursor"]["firstBatch"],
            "ns": result["cursor"]["ns"],
        }

    def __aiter__(self) -> Cursor:
        """Get the cursor as an async iterator."""
        return self

    async def __anext__(self) -> dict[str, Any]:
        """Get the next document from the cursor."""
        try:
            return await self.next()
        except CursorIsEmptyError as e:
            raise StopAsyncIteration from e

    def __repr__(self) -> str:
        """Get the string representation of the cursor."""
        return f"<Cursor {self._cursor['ns']}#{self._cursor['id']}>"

    async def next(self) -> dict[str, Any]:
        """Get the next document from the cursor.

        Raises
            Empty: If there are no more documents in the cursor.

        Returns
            dict[str, Any]: The next document.
        """
        while self._cursor.get("nextBatch"):
            if self._cursor["nextBatch"]:
                return self._cursor["nextBatch"].pop(0)

            self._cursor = await self._connection._send_and_wait(
                {
                    "getMore": self._cursor["id"],
                    "collection": self._cursor["ns"],
                    "$db": self._connection._uri.path[1:],
                }
            )

        raise CursorIsEmptyError
