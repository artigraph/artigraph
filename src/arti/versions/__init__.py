from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import inspect
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated, Any, cast

from annotated_types import Ge, Timezone
from pydantic import Field, field_validator

from arti.internal.models import Model


class Version(Model):
    _abstract_ = True


class GitCommit(Version):
    sha: str = Field(
        default_factory=(
            lambda: (
                subprocess.check_output(["git", "rev-parse", "HEAD"])  # noqa: S607
                .decode()
                .strip()
            )
        )
    )


class SemVer(Version):
    """
    SemVer fingerprinting only considers the major component, unless it is 0.

    By only considering the major version, we can add incremental bumps to a Producer without triggering
    historical backfills. The major version MUST be incremented on schema or methodological changes.
    Before 1.0.0, the minor and patch components are also fingerprinted.
    """

    major: Annotated[int, Ge(0)]
    minor: Annotated[int, Ge(0)]
    patch: Annotated[int, Ge(0)]

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        fields = set(cls._arti_fingerprint_fields_)
        if unexpected := fields - {"_arti_type_", "major", "minor", "patch"}:
            raise NotImplementedError(
                f"Unexpected {cls._arti_type_key_} fingerprint fields ({unexpected}) - did we forget to handle them?"
            )

    def _fingerprint_fields(self) -> tuple[str, ...]:
        fields = super()._fingerprint_fields()
        if self.major == 0:
            return fields
        return tuple(f for f in fields if f not in {"minor", "patch"})


class String(Version):
    value: str


class _SourceDescriptor:  # Experimental :)
    # Using AST rather than literal source will likely be less "noisy":
    #     https://github.com/artigraph/artigraph/pull/36#issuecomment-824131156
    def __get__(self, obj: Any, type_: type) -> String:
        return String(value=inspect.getsource(type_))


_Source = cast(Callable[[], String], _SourceDescriptor)


class Timestamp(Version):
    # The Timezone(...) annotation should tell pydantic a timezone is required, but it may not be
    # supported yet so we also validate it manually in `_requires_timezone`.
    dt: Annotated[datetime, Timezone(...)] = Field(
        default_factory=lambda: datetime.now(tz=UTC), validate_default=True
    )

    @field_validator("dt")
    @classmethod
    def _requires_timezone(cls, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            raise ValueError("Timestamp requires a timezone-aware datetime!")
        return dt


# TODO: Consider a Timestamp like version with a "frequency" arg (day, hour, etc) that we floor/ceil
# to trigger "scheduled" invalidation. This doesn't solve imperative work like "process X every hour
# on the hour and play catch up for missed runs" (rather, we should aim to represent that with
# upstream Artifact partitions), but rather solves work like "ingest all data at most this
# frequently" (eg: full export from an API - no point in a backfill).
