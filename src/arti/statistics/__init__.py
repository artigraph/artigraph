from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from arti.internal.models import Model


class Statistic(Model):
    pass  # TODO: Determine the interface for Statistics


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
