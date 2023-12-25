# SPDX-License-Identifier: MIT

from typing import NamedTuple


class MessageHeader(NamedTuple):
    message_length: int
    request_id: int
    response_to: int
    opcode: int
