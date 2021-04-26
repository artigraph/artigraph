from __future__ import annotations

from arti.artifacts.core import BaseArtifact


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
#     schema = Int64()
#
#
# class CountDistinct(FieldStatistic):
#     schema = Int64()
#
#
# class MaxInt64(FieldStatistic):
#     schema = Int64()
#
#
# class MinInt64(FieldStatistic):
#     schema = Int64()
#
#
# class SumInt64(FieldStatistic):
#     schema = Int64()
