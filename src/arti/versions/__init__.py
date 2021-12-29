__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import inspect
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, cast

from pydantic import Field, validator

from arti.fingerprints import Fingerprint
from arti.internal.models import Model


class Version(Model):
    _abstract_ = True


class GitCommit(Version):
    sha: str = Field(
        default_factory=(
            lambda: subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        )
    )


class SemVer(Version):
    """SemVer fingerprinting only considers the major component, unless it is less than 0.

    By only considering the major version, we can add incremental bumps to a Producer without triggering
    historical backfills. The major version MUST be incremented on schema or methodological changes.
    """

    major: int
    minor: int
    patch: int

    @property
    def fingerprint(self) -> Fingerprint:
        s = str(self.major)
        if self.major == 0:
            s = f"{self.major}.{self.minor}.{self.patch}"
        return Fingerprint.from_string(s)


class String(Version):
    value: str


class _SourceDescriptor:  # Experimental :)
    # Using AST rather than literal source will likely be less "noisy":
    #     https://github.com/artigraph/artigraph/pull/36#issuecomment-824131156
    def __get__(self, obj: Any, type_: type) -> String:
        return String(value=inspect.getsource(type_))


_Source = cast(Callable[[], String], _SourceDescriptor)


class Timestamp(Version):
    dt: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @validator("dt", always=True)
    @classmethod
    def _requires_timezone(cls, dt: datetime) -> datetime:
        if dt is not None and dt.tzinfo is None:
            raise ValueError("Timestamp requires a timezone-aware datetime!")
        return dt

    @property
    def fingerprint(self) -> Fingerprint:
        return Fingerprint.from_int(round((self.dt or datetime.utcnow()).timestamp()))


# TODO: Consider a Timestamp like version with a "frequency" arg (day, hour, etc) that we floor/ceil
# to trigger "scheduled" invalidation. This doesn't solve imperative work like "process X every hour
# on the hour and play catch up for missed runs" (rather, we should aim to represent that with
# upstream Artifact partitions), but rather solves work like "ingest all data at most this
# frequently" (eg: full export from an API - no point in a backfill).
