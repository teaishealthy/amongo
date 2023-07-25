from datetime import datetime
from typing import Any, TypedDict


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
