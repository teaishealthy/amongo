# SPDX-License-Identifier: MIT

"""Connection to a MongoDB server."""
import asyncio
import random
import struct
from asyncio import StreamReader, StreamWriter
from typing import Any, NamedTuple, TypeVar
from urllib.parse import ParseResult, urlparse

import bson

from ._flags import Flags
from ._header import MessageHeader
from ._hello import Hello
from .collection import Collection

T = TypeVar("T")


class _WireItem(NamedTuple):
    header: MessageHeader
    data: bytes


class Connection:
    """Connection to a MongoDB server."""

    def __init__(self, uri: str) -> None:
        """Create a new Connection instance.

        Args:
            uri (str): The URI to connect to.
        """
        self.__reader: StreamReader | None = None
        self.__writer: StreamWriter | None = None
        self._uri: ParseResult = urlparse(uri)
        self.__hello: Hello | None = None
        self._task: asyncio.Task[None] | None = None
        self._waiters: dict[int, asyncio.Future[_WireItem]] = {}

    def _fail_if_none(self, value: T | None) -> T:
        if value is None:
            msg = "Connection not established. Did you forget to call `open()`?"
            raise RuntimeError(msg)
        return value

    @property
    def _reader(self) -> StreamReader:
        return self._fail_if_none(self.__reader)

    @property
    def _writer(self) -> StreamWriter:
        return self._fail_if_none(self.__writer)

    @property
    def _hello(self) -> Hello:
        return self._fail_if_none(self.__hello)

    async def _keep_reading(self) -> None:
        while True:
            header_data = await self._reader.readexactly(16)
            header = MessageHeader(
                *struct.unpack("<iiii", header_data),
            )
            length = header.message_length - 16
            data = await self._reader.read(length)

            if waiter := self._waiters.pop(header.response_to, None):
                waiter.set_result(_WireItem(header, data))

    async def _wait_for_response(self, request_id: int) -> _WireItem:
        future = asyncio.get_running_loop().create_future()
        self._waiters[request_id] = future
        return await future

    def _parse_data(self, data: _WireItem) -> Any:
        """Parse the data from a WireItem.

        Args:
            data (WireItem): The data to parse.

        Returns:
            Any: The parsed data. This will be decoded from BSON.
        """
        assert (  # noqa: S101 TODO
            data.header.opcode == 2013
        ), "Expected OP_MSG, are you sure you are using MongoDB 5.1+?"

        flags_bits = struct.unpack("<i", data.data[:4])[0]
        _flags = Flags(flags_bits).verify()

        kind = data.data[4]
        if kind != 0:
            msg = "Only Sections of type 0 / body are supported"
            raise NotImplementedError(msg)
            # TODO: Implement other types of sections

        return bson.loads(data.data[5:])

    async def _send_and_wait(self, data: Any) -> Any:
        """Send an OP_MSG with kind 0 and wait for the matching response.

        Args:
            data (Any): The data to send, this will be encoded as BSON.

        Returns:
            Any: The response data, this will be decoded from BSON.
        """
        header = await self._send(data)
        return self._parse_data(await self._wait_for_response(header.request_id))

    async def _send(self, data: Any) -> MessageHeader:
        data_bytes = b""
        flags = 0

        data_bytes += int(flags).to_bytes(4, "little")
        data_bytes += int(0).to_bytes(1, "little")  # kind
        data_bytes += bson.dumps(data)  # payload

        header = MessageHeader(
            message_length=16 + len(data_bytes),
            request_id=random.randint(-(2**31) + 1, 2**31 - 1),
            response_to=0,
            opcode=2013,
        )

        self._writer.write(struct.pack("<iiii", *header) + data_bytes)
        await self._writer.drain()
        return header

    async def open(self) -> None:
        """Open the connection."""
        self.__reader, self.__writer = await asyncio.open_connection(
            self._uri.hostname, self._uri.port or 27017
        )

        result = await self._send({"hello": 1, "$db": self._uri.path[1:] or "admin"})
        future = asyncio.get_running_loop().create_future()
        self._waiters[result.request_id] = future

        self._task = asyncio.create_task(self._keep_reading())
        self.__hello = self._parse_data(await future)

    def use(self, database: str) -> None:
        """Change the database to use.

        Args:
            database (str): The name of the database to use.
        """
        self._uri = self._uri._replace(path=f"/{database}")

    def coll(self, collection: str) -> Collection:
        """Get a collection.

        Args:
            collection (str): The name of the collection to get.

        Returns:
            Collection: The collection.
        """
        return Collection(self, collection)
