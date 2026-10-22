from typing import Annotated, assert_type

import pytest
from pydantic import ValidationError

from arti import Artifact, Producer, Storage, Type, producer
from arti.storage.local import LocalFile
from arti.types import Int64


class Input(Artifact):
    type: Type = Int64()


class Output(Artifact):
    type: Type = Int64()
    storage: Storage = LocalFile()


@producer()
def increment(value: Annotated[int, Input]) -> Annotated[int, Output]:
    return value + 1


class Increment(Producer):
    value: Input

    @staticmethod
    def build(value: int) -> Annotated[int, Output]:
        return value + 1


def test_strict_consumer_api() -> None:
    assert_type(increment, type[Producer])
    assert issubclass(increment, Producer)

    task = increment(value=Input())

    assert_type(task, Producer)
    assert isinstance(task, Producer)
    assert isinstance(task.out(), Output)
    assert isinstance(Output().storage, LocalFile)
    with pytest.raises(ValidationError):
        increment(value=1)  # pyright: ignore[reportArgumentType]


def test_strict_class_producer_api() -> None:
    assert_type(Increment(value=Input()), Increment)
    # Class-based Producers keep pydantic's precise `__init__`, so the wrong Artifact is a type error.
    with pytest.raises(ValidationError):
        Increment(value=Output())  # pyright: ignore[reportArgumentType]
