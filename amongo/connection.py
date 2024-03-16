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
from urllib.parse import ParseResult, parse_qs, urlparse

import bson

from .collection import Collection
from .core.compressors import compression_lookup, list_compressors, pick_compressor
from .core.models import ConnectionOptions, Flags, MessageHeader, WireItem
from .core.typings import MessageOpCode, MessageSectionKind

if TYPE_CHECKING:
    from .core.typings import Hello

T = TypeVar("T")

logger = logging.getLogger(__name__)

MAX_WRITE_BATCH_SIZE = 1000


def bson_dumps(data: Any) -> bytes:
    """Encode data as BSON.

    Args:
        data (Any): The data to encode.

    Returns:
        bytes: The encoded data.
    """
    return bson.encode(data)


def bson_loads(data: bytes) -> Any:
    """Decode BSON data.

    Args:
        data (bytes): The data to decode.

    Returns:
        Any: The decoded data.
    """
    return bson.decode(data)  # type: ignore


def make_data(
    data: Any, *, max_write_batch_size: int, flags: int, list_key: str | None = None
) -> io.BytesIO:
    """Make a data section for an OP_MSG.

    Args:
        data (Any): The data to encode.
        max_write_batch_size (int): The maximum number of documents that can be inserted in a single batch.
        flags (int): The flags to use.
        list_key (str | None, optional): The key to use for a list of documents.

    Returns:
        io.BytesIO: The encoded data.
    """  # noqa: E501
    arr: list[Any] | None = None
    if list_key is not None:
        arr = data.pop(list_key)

    data_bytes = io.BytesIO()
    data_bytes.write(struct.pack("<I", flags))

    # sections:
    data_bytes.write(struct.pack("<B", 0))  # section kind
    data_bytes.write(bson_dumps(data))

    if arr is not None and list_key is not None:
        idx = 0
        while idx < len(arr):
            section_writer = io.BytesIO()

            data_bytes.write(struct.pack("<B", 1))  # section kind

            section_writer.write(list_key.encode("utf-8"))
            section_writer.write(b"\x00")

            while idx < len(arr):
                section_writer.write(bson_dumps(arr[idx]))
                idx += 1

                if idx >= max_write_batch_size:
                    break

            data_bytes.write(struct.pack("<I", section_writer.tell() + 4))
            data_bytes.write(section_writer.getvalue())

    return data_bytes


async def parse_header(reader: asyncio.StreamReader) -> WireItem:
    """Parse a message header and load the data.

    Args:
        reader (asyncio.StreamReader): The reader to read from.

    Returns:
        WireItem: The parsed header and data.
    """
    header_data = await reader.readexactly(16)
    header = MessageHeader(
        *struct.unpack("<iiii", header_data),
    )
    length = header.message_length - 16
    data = await reader.read(length)
    return WireItem(header, data)


async def parse_data(data: WireItem) -> Any:  # noqa: C901
    """Parse the data from a WireItem.

    Args:
        data (WireItem): The data to parse.

    Returns:
        Any: The parsed data. This will be decoded from BSON.
    """
    logger.debug("< %s", data.header)
    if data.header.opcode == MessageOpCode.OP_COMPRESSED:
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
        decompressed_data = await asyncio.get_event_loop().run_in_executor(
            None, compressor().decompress, data.data[9:]
        )

        if len(decompressed_data) != uncompressed_length:
            msg = "Decompressed data is not the expected length"
            raise RuntimeError(msg)

        data = WireItem(
            data.header._replace(opcode=original_opcode),
            decompressed_data,
        )

    if data.header.opcode != MessageOpCode.OP_MESSAGE:
        msg = "Only OP_MSG is supported"
        raise NotImplementedError(msg)

    (flags_bits,) = struct.unpack("<i", data.data[:4])
    _flags = Flags(flags_bits).verify()

    body: Any | None = None
    reader = io.BytesIO(data.data[4:])

    while reader.tell() < len(data.data[4:]):
        (kind,) = struct.unpack("<B", reader.read(1))

        if kind == MessageSectionKind.BODY:
            if body is not None:
                msg = (
                    "Expected only one body section, but found multiple\n",
                    "This is a bug in amongo, please report it at \n",
                    "https://github.com/teaishealthy/amongo/issues/new",
                )
                raise NotImplementedError(msg)

            # This is part of the BSON spec, not the MongoDB wire protocol
            (length,) = struct.unpack("<i", reader.read(4))
            reader.seek(-4, io.SEEK_CUR)
            body = bson_loads(reader.read(length))

        elif kind == MessageSectionKind.DOCUMENT_SEQUENCE:
            if body is None:
                msg = "Body section must come before document sequence"
                raise RuntimeError(msg)

            (size,) = struct.unpack("<i", reader.read(4))

            string_bytes = bytearray()
            while (byte := reader.read(1)) != b"\x00":
                string_bytes += byte

            string = string_bytes.decode("utf-8")

            sequence: list[Any] = []
            if body.get(string) is None:
                body[string] = sequence

            sequence.extend(
                bson.decode_all(  # type: ignore
                    reader.read(
                        size - len(string_bytes) - 1,
                    )
                )
            )

    return body


class Connection:
    """Connection to a MongoDB server."""

    @property
    def max_write_batch_size(self) -> int:
        """Get the maximum number of documents that can be inserted in a single batch.

        Returns:
            int: The maximum number of documents that can be inserted in a single batch.
        """
        if self.__hello is None:
            return MAX_WRITE_BATCH_SIZE
        return self.__hello["maxWriteBatchSize"]

    def __init__(self, uri: str) -> None:
        """Create a new Connection instance.

        Args:
            uri (str): The URI to connect to.
        """
        self.__reader: StreamReader | None = None
        self.__writer: StreamWriter | None = None
        self._uri: ParseResult = urlparse(uri)
        query_string = parse_qs(self._uri.query)
        compressors = query_string.get("compressors", None)

        if compressors is not None:
            compressors = compressors[0].split(",")

        self._options: ConnectionOptions = ConnectionOptions(compressors=compressors)
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
            item = await parse_header(self._reader)

            if waiter := self._waiters.pop(item.header.response_to, None):
                waiter.set_result(item)

    async def _wait_for_response(self, request_id: int) -> WireItem:
        future = asyncio.get_running_loop().create_future()
        self._waiters[request_id] = future
        return await future

    async def _send_and_wait(self, data: Any, list_key: str | None = None) -> Any:
        """Send an OP_MSG with kind 0 and wait for the matching response.

        Args:
            data (Any): The data to send, this will be encoded as BSON.
            list_key (str | None, optional): The key to use for a list of documents.

        Returns:
            Any: The response data, this will be decoded from BSON.
        """
        header = await self._send(data, list_key)
        return await parse_data(await self._wait_for_response(header.request_id))

    async def _send(self, data: Any, list_key: str | None = None) -> MessageHeader:
        if self.__hello and self.__hello.get("compression"):
            return await self._send_compressed(data, list_key)

        data_bytes = make_data(
            data,
            flags=0,
            max_write_batch_size=self.max_write_batch_size,
            list_key=list_key,
        )

        header = MessageHeader(
            message_length=16 + data_bytes.tell(),
            request_id=random.randint(-(2**31) + 1, 2**31 - 1),
            response_to=0,
            opcode=MessageOpCode.OP_MESSAGE,
        )

        self._writer.write(struct.pack("<iiii", *header))
        self._writer.write(data_bytes.getvalue())
        await self._writer.drain()
        return header

    async def _send_compressed(
        self, data: Any, list_key: str | None = None
    ) -> MessageHeader:
        compressor, compressor_id = pick_compressor(self._hello["compression"])

        original_data = make_data(
            data,
            flags=0,
            max_write_batch_size=self.max_write_batch_size,
            list_key=list_key,
        )
        data_bytes = io.BytesIO()

        data_bytes.write(
            struct.pack(
                "<IIB",
                MessageOpCode.OP_MESSAGE,
                original_data.tell(),
                compressor_id,
            ),
        )

        compressed = await asyncio.get_running_loop().run_in_executor(
            None, compressor().compress, original_data.getvalue()
        )

        data_bytes.write(compressed)

        header = MessageHeader(
            message_length=16 + data_bytes.tell(),
            request_id=random.randint(-(2**31) + 1, 2**31 - 1),
            response_to=0,
            opcode=MessageOpCode.OP_COMPRESSED,
        )

        logger.debug("> %s", header)
        logger.debug("  compressing with %s", compressor.name)

        self._writer.write(struct.pack("<iiii", *header))
        self._writer.write(data_bytes.getvalue())
        await self._writer.drain()

        return header

    async def open(self) -> None:
        """Open the connection."""
        self.__reader, self.__writer = await asyncio.open_connection(
            self._uri.hostname,
            self._uri.port or 27017,
        )

        command: dict[str, Any] = {
            "hello": 1,
            "$db": self._uri.path[1:] or "admin",
        }

        if self._options.compressors is None:
            logger.debug("Sending hello.")
        else:
            logger.debug(
                "Sending hello. Available compressors: %s",
                ", ".join(self._options.compressors),
            )
            command["compression"] = [
                c for c in list_compressors() if c in self._options.compressors
            ]

        result = await self._send(command)
        future = asyncio.get_running_loop().create_future()
        self._waiters[result.request_id] = future

        self._task = asyncio.create_task(self._keep_reading())
        self.__hello = await parse_data(await future)

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
