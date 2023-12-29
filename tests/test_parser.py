import random
import struct
from asyncio import StreamReader

import pytest

from amongo.connection import make_data, parse_data, parse_header
from amongo.core.models import MessageHeader
from amongo.core.typings import MessageOpCode

EXAMPLE_DATA = {
    "foo": "bar",
    "spam": "eggs",
}


@pytest.mark.asyncio()
async def test_parser() -> None:
    reader = StreamReader()
    data = make_data(EXAMPLE_DATA, max_write_batch_size=1000, flags=0)

    header = MessageHeader(
        message_length=16 + data.tell(),
        request_id=random.randint(-(2**31) + 1, 2**31 - 1),
        response_to=0,
        opcode=MessageOpCode.OP_MESSAGE,
    )
    reader.feed_data(struct.pack("<iiii", *header))
    reader.feed_data(data.getvalue())
    reader.feed_eof()

    item = await parse_header(reader)
    parsed_data = await parse_data(item)

    assert parsed_data == EXAMPLE_DATA


@pytest.mark.asyncio()
async def test_parser_document_sequence() -> None:
    reader = StreamReader()
    data = make_data(
        {
            "documents": [
                EXAMPLE_DATA,
                EXAMPLE_DATA,
                EXAMPLE_DATA,
            ]
        },
        max_write_batch_size=1000,
        flags=0,
    )

    header = MessageHeader(
        message_length=16 + data.tell(),
        request_id=random.randint(-(2**31) + 1, 2**31 - 1),
        response_to=0,
        opcode=MessageOpCode.OP_MESSAGE,
    )
    reader.feed_data(struct.pack("<iiii", *header))
    reader.feed_data(data.getvalue())
    reader.feed_eof()

    item = await parse_header(reader)
    parsed_data = await parse_data(item)

    assert parsed_data == {
        "documents": [
            EXAMPLE_DATA,
            EXAMPLE_DATA,
            EXAMPLE_DATA,
        ]
    }
