# SPDX-License-Identifier: MIT
from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from datetime import datetime


class Hello(TypedDict):
    isWritablePrimary: bool
    topologyVersion: Any
    maxBsonObjectSize: int
    maxMessageSizeBytes: int
    maxWriteBatchSize: int
    localTime: datetime
    logicalSessionTimeoutMinutes: int
    connectionId: int
    minWireVersion: int
    maxWireVersion: int
    readOnly: bool
    compression: list[str]
    saslSupportedMechs: list[str]