# SPDX-License-Identifier: MIT

"""Connection to a MongoDB server."""
from __future__ import annotations

import asyncio
import io
import logging
import random
import struct
from asyncio import StreamReader, StreamWriter
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import ParseResult, urlparse

import bson

from .collection import Collection
from .core.compressors import compression_lookup, list_compressors, pick_compressor
from .core.models import Flags, MessageHeader, WireItem

if TYPE_CHECKING:
    from .core.typings import Hello

T = TypeVar("T")

logger = logging.getLogger(__name__)


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
        self._waiters: dict[int, asyncio.Future[WireItem]] = {}

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
                waiter.set_result(WireItem(header, data))

    async def _wait_for_response(self, request_id: int) -> WireItem:
        future = asyncio.get_running_loop().create_future()
        self._waiters[request_id] = future
        return await future

    def _parse_data(self, data: WireItem) -> Any:
        """Parse the data from a WireItem.

        Args:
            data (WireItem): The data to parse.

        Returns:
            Any: The parsed data. This will be decoded from BSON.
        """
        logger.debug("< %s", data.header)
        if data.header.opcode == 2012:
            (
                original_opcode,
                uncompressed_length,
                compressor_id,
            ) = struct.unpack("<iib", data.data[:9])
            compressor = compression_lookup[compressor_id]

            logger.debug(
                "  decompressing with %s",
                compressor.name,
            )
            decompressed_data = compressor().decompress(data.data[9:])

            if len(decompressed_data) != uncompressed_length:
                msg = "Decompressed data is not the expected length"
                raise RuntimeError(msg)

            data = WireItem(
                data.header._replace(opcode=original_opcode),
                decompressed_data,
            )

        if data.header.opcode != 2013:
            msg = "Only OP_MSG is supported"
            raise NotImplementedError(msg)

        (flags_bits,) = struct.unpack("<i", data.data[:4])
        _flags = Flags(flags_bits).verify()

        kind = data.data[4]
        if kind != 0:
            msg = "Only Sections of type 0 / body are supported"
            raise NotImplementedError(msg)
            # TODO @teaishealthy: Implement other types of sections

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

    def _make_data(self, data: Any, *, flags: int) -> io.BytesIO:
        data_bytes = io.BytesIO()
        data_bytes.write(struct.pack("<I", flags))

        # sections:
        data_bytes.write(struct.pack("<B", 0))  # section kind
        data_bytes.write(bson.dumps(data))

        return data_bytes

    async def _send(self, data: Any) -> MessageHeader:
        if self.__hello and self.__hello.get("compression"):
            return await self._send_compressed(data)

        data_bytes = self._make_data(data, flags=0)

        header = MessageHeader(
            message_length=16 + data_bytes.getbuffer().nbytes,
            request_id=random.randint(-(2**31) + 1, 2**31 - 1),
            response_to=0,
            opcode=2013,
        )

        self._writer.write(struct.pack("<iiii", *header) + data_bytes.getvalue())
        await self._writer.drain()
        return header

    async def _send_compressed(self, data: Any) -> MessageHeader:
        compressor, compressor_id = pick_compressor(self._hello["compression"])

        original_data = self._make_data(data, flags=0)
        data_bytes = io.BytesIO()

        data_bytes.write(
            struct.pack(
                "<IIB",
                2013,
                original_data.getbuffer().nbytes,
                compressor_id,
            ),
        )

        data_bytes.write(compressor().compress(original_data.getvalue()))

        header = MessageHeader(
            message_length=16 + data_bytes.getbuffer().nbytes,
            request_id=random.randint(-(2**31) + 1, 2**31 - 1),
            response_to=0,
            opcode=2012,
        )

        logger.debug("> %s", header)
        logger.debug("  compressing with %s", compressor.name)

        self._writer.write(struct.pack("<iiii", *header) + data_bytes.getvalue())
        await self._writer.drain()

        return header

    async def open(self) -> None:
        """Open the connection."""
        self.__reader, self.__writer = await asyncio.open_connection(
            self._uri.hostname,
            self._uri.port or 27017,
        )

        logger.debug(
            "Sending hello. Available compressors: %s", ", ".join(list_compressors())
        )
        result = await self._send(
            {
                "hello": 1,
                "$db": self._uri.path[1:] or "admin",
                "compression": list_compressors(),
            },
        )
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
