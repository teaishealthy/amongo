from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from .typings import Document

T = TypeVar("T")


@dataclass
class Result:
    ok: bool

    @classmethod
    @abstractmethod
    def from_response(cls: type[T], response: Document) -> T:
        return cls(ok=response["ok"] == 1)


@dataclass
class DeleteResult(Result):
    """The result of a delete operation."""

    n: int
    """The number of documents deleted."""

    write_errors: list[Document]
    """A list of write errors."""

    write_concern_error: Document
    """A write concern error."""

    @classmethod
    def from_response(cls: type[T], response: Document) -> T:
        return cls(
            ok=response["ok"] == 1,
            n=response["n"],
            write_errors=response.get("writeErrors", []),
            write_concern_error=response.get("writeConcernError", {}),
        )
