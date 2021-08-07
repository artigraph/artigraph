from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from arti.artifacts import BaseArtifact


class Statistic(BaseArtifact):
    """A Statistic is a piece of data derived from an Artifact that can be tracked over time."""

    # TODO: Set format/storage to some "system default" that can be used across backends.

    is_scalar = True


# class FieldStatistic(Statistic):
#     """ A FieldStatistic is a Statistic associated with a particular Artifact field.
#     """
#
#
# class Count(FieldStatistic):
#     type = Int64()
#
#
# class CountDistinct(FieldStatistic):
#     type = Int64()
#
#
# class MaxInt64(FieldStatistic):
#     type = Int64()
#
#
# class MinInt64(FieldStatistic):
#     type = Int64()
#
#
# class SumInt64(FieldStatistic):
#     type = Int64()
