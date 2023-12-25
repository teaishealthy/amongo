# SPDX-License-Identifier: MIT

"""A MongoDB collection."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from amongo.cursor import Cursor

if TYPE_CHECKING:
    from .connection import Connection


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

    async def rename(self, new_name: str) -> None:
        """Rename the collection.

        Args:
            new_name (str): The new name of the collection.
        """
        await self._connection._send(
            {
                "renameCollection": f"{self._db}.{self._name}",
                "to": f"{self._db}.{new_name}",
                "$db": self._db,
            },
        )

    async def delete(
        self,
        q: dict[str, Any],
        limit: int = 0,
    ) -> int:
        """Delete one or more documents.

        Args:
            q (dict[str, Any]): The query that matches documents to delete.
            limit (int, optional): The number of matching documents to delete.
            Specify either a 0 to delete all matching documents or 1 to delete a single document.
            Defaults to 0.

        Returns:
            int: The number of documents deleted.
        """  # noqa: E501
        # TODO @teaishealthy: collation and hint?
        result = await self._connection._send_and_wait(
            {
                "delete": self._name,
                "deletes": [
                    {
                        "q": q,
                        "limit": limit,
                    },
                ],
                "$db": self._db,
            },
        )

        return result["n"]

    async def delete_one(self, q: dict[str, Any]) -> int:
        """Delete a single document.

        Args:
            q (dict[str, Any]): The query that matches the document to delete.

        Returns:
            int: The number of documents deleted.
        """
        return await self.delete(q, 1)

    async def insert_many(self, documents: list[dict[str, Any]]) -> int:
        """Insert one or more documents.

        Args:
            documents (list[dict[str, Any]]): The documents to insert.

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

    async def insert_one(self, document: dict[str, Any]) -> int:
        """Insert a single document.

        Args:
            document (dict[str, Any]): The document to insert.

        Returns:
            int: The number of documents inserted.
        """
        return await self.insert_many([document])

    async def find(
        self,
        q: dict[str, Any],
        skip: int | None = None,
        limit: int | None = None,
        max: int | None = None,
        min: int | None = None,
    ) -> Cursor:
        """Select documents from the collection.

        Args:
            q (dict[str, Any]): The query that matches documents to find.
            skip (int | None, optional): The number of documents to skip. Defaults to None.
            limit (int | None, optional): The maximum number of documents to return. Defaults to None.
            max (int | None, optional): The maximum value of the index to use. Defaults to None.
            min (int | None, optional): The minimum value of the index to use. Defaults to None.

        Returns:
            Cursor: The cursor to iterate over the documents.
        """  # noqa: E501
        doc: dict[str, Any] = {
            "find": self._name,
            "filter": q,
            "$db": self._db,
        }

        if skip is not None:
            doc["skip"] = skip

        if limit is not None:
            doc["limit"] = limit

        if max is not None:
            doc["max"] = max

        if min is not None:
            doc["min"] = min

        result = await self._connection._send_and_wait(doc)
        return Cursor(self._connection, result)

    async def find_one(self, q: dict[str, Any]) -> dict[str, Any]:
        """Select a single document from the collection.

        Args:
            q (dict[str, Any]): The query that matches the document to find.

        Returns:
            dict[str, Any]: The document found.
        """
        return await (await self.find(q)).next()
