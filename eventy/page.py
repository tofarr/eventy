from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    """Page of items"""

    items: list[T]
    next_page_id: str | None = None
