# SPDX-License-Identifier: MIT

"""A MongoDB collection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .core.errors import CursorIsEmptyError, DatabaseError
from .core.results import DeleteResult
from .cursor import Cursor

if TYPE_CHECKING:
    from .connection import Connection
    from .core.typings import Document


class Collection:
    """A MongoDB collection."""

    def __init__(self, connection: Connection, name: str) -> None:
        """Create a new Collection instance. This should not be called directly.

        Args:
            connection (Connection): The connection to use.
            name (str): The name of the collection.
        """
        self._connection = connection
        self._db = connection._uri.path[1:]
        # We need to store the database name incase the connection changes databases
        self._name = name

    def __repr__(self) -> str:
        """Get the string representation of the collection."""
        return f"<Collection {self._name!r}>"

    async def rename(
        self,
        name: str,
        *,
        db: str | None = None,
        drop_target: bool = False,
    ) -> None:
        """Rename the collection.

        Args:
            name (str): The new name of the collection.
            db (str | None, optional): The database to rename the collection in.
                Uses the current database if None. Defaults to None.
            drop_target (bool, optional): Whether to drop the target collection if it exists. Defaults to False.

        Raises:
            DatabaseError: If the operation fails.
        """  # noqa: E501
        if db is None:
            db = self._db

        command: Document = {
            "renameCollection": f"{self._db}.{self._name}",
            "to": f"{db}.{name}",
            "dropTarget": drop_target,
            "$db": "admin",
        }

        result = await self._connection._send_and_wait(
            command,
        )

        if result["ok"] != 1:
            raise DatabaseError(result)

    async def drop(self) -> None:
        """Drop the collection."""
        result = await self._connection._send_and_wait(
            {
                "drop": self._name,
            },
        )

        if result["ok"] != 1:
            raise DatabaseError(result)

    async def delete(
        self,
        q: Document,
        *,
        limit: int = 0,
        ordered: bool = True,
    ) -> DeleteResult:
        """Delete one or more documents.

        Args:
            q (Document): The query that matches documents to delete.
            limit (int, optional): The number of matching documents to delete.
                Specify 0 to delete all matching documents. Defaults to 0.
            ordered (bool, optional): Whether to stop deleting documents after an error occurs. Defaults to True.

        Raises:
            DatabaseError: If the operation fails.

        Returns:
            int: The number of documents deleted.
        """  # noqa: E501
        # TODO @teaishealthy: collation and hint?

        command: Document = {
            "delete": self._name,
            "deletes": [
                {
                    "q": q,
                    "limit": limit,
                },
            ],
            "ordered": ordered,
            "$db": self._db,
        }

        result: Document = await self._connection._send_and_wait(command)

        if result["ok"] != 1:
            raise DatabaseError(result)

        return DeleteResult.from_response(result)

    async def delete_one(self, q: Document) -> DeleteResult:
        """Delete a single document.

        Args:
            q (Document): The query that matches the document to delete.

        Returns:
            int: The number of documents deleted.
        """
        return await self.delete(q, limit=1)

    async def insert_many(self, documents: list[Document]) -> int:
        """Insert one or more documents.

        Args:
            documents (list[Document]): The documents to insert.

        Returns:
            int: The number of documents inserted.
        """
        result = await self._connection._send_and_wait(
            {
                "insert": self._name,
                "documents": documents,
                "$db": self._db,
            },
        )
        return result["n"]

    async def insert_one(self, document: Document) -> int:
        """Insert a single document.

        Args:
            document (Document): The document to insert.

        Returns:
            int: The number of documents inserted.
        """
        return await self.insert_many([document])

    async def find(
        self,
        q: Document,
        skip: int | None = None,
        limit: int = 0,
        max: int | None = None,
        min: int | None = None,
    ) -> Cursor:
        """Select documents from the collection.

        Args:
            q (Document): The query that matches documents to find.
            skip (int | None, optional): The number of documents to skip. Defaults to None.
            limit (int, optional): The maximum number of documents to return. Defaults to 0.
            max (int | None, optional): The maximum value of the index to use. Defaults to None.
            min (int | None, optional): The minimum value of the index to use. Defaults to None.

        Returns:
            Cursor: The cursor to iterate over the documents.
        """  # noqa: E501
        doc: Document = {
            "find": self._name,
            "filter": q,
            "$db": self._db,
            "limit": limit,
        }

        if skip is not None:
            doc["skip"] = skip

        if max is not None:
            doc["max"] = max

        if min is not None:
            doc["min"] = min

        result = await self._connection._send_and_wait(doc)
        return Cursor(self._connection, result)

    async def find_one(self, q: Document) -> Document | None:
        """Select a single document from the collection.

        Args:
            q (Document): The query that matches the document to find.

        Returns:
            Document | None: The document if found, otherwise None.
        """
        cursor = await self.find(q, limit=1)
        try:
            return await cursor.next()
        except CursorIsEmptyError:
            return None
