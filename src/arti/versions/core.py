import inspect
import os
import subprocess
from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional, cast

from arti.fingerprints.core import Fingerprint
from arti.internal.utils import qname


class Version:
    @property
    def fingerprint(self) -> Fingerprint:
        raise NotImplementedError(f"{qname(self)}.fingerprint is not implemented!")


class GitCommit(Version):
    def __init__(self, *, envvar: str = "GIT_SHA"):
        if (sha := os.environ.get(envvar)) is None:
            sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        self.sha = sha

    @property
    def fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.sha)


class SemVer(Version):
    """ SemVer fingerprinting only considers the major component, unless it is less than 0.

        By only considering the major version, we can add incremental bumps to a Producer without triggering
        historical backfills. The major version MUST be incremented on schema or methodological changes.
    """

    def __init__(self, major: int, minor: int, patch: int):
        self.major = major
        self.minor = minor
        self.patch = patch

    @property
    def fingerprint(self) -> Fingerprint:
        s = str(self.major)
        if self.major == 0:
            s = f"{self.major}.{self.minor}.{self.patch}"
        return Fingerprint.from_string(s)


class String(Version):
    def __init__(self, value: str):
        self.value = value

    @property
    def fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.value)


class _SourceDescriptor:  # Experimental :)
    # Using AST rather than literal source will likely be less "noisy":
    #     https://github.com/replicahq/artigraph/pull/36#issuecomment-824131156
    def __get__(self, obj: Any, type_: type) -> String:
        return String(inspect.getsource(type_))


_Source = cast(Callable[[], String], _SourceDescriptor)


class Timestamp(Version):
    def __init__(self, dt: Optional[datetime] = None):
        if dt is not None and dt.tzinfo is None:
            raise ValueError("Timestamp requires a timezone-aware datetime!")
        self.dt = dt

    @property
    def fingerprint(self) -> Fingerprint:
        return Fingerprint.from_int(round((self.dt or datetime.utcnow()).timestamp()))


# TODO: Consider a Timestamp like version with a "frequency" arg (day, hour, etc) that we floor/ceil
# to trigger "scheduled" invalidation. This doesn't solve imperative work like "process X every hour
# on the hour and play catch up for missed runs" (rather, we should aim to represent that with
# upstream Artifact partitions), but rather solves work like "ingest all data at most this
# frequently" (eg: full export from an API - no point in a backfill).
