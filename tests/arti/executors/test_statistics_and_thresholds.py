from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated, Any, TypedDict, cast

import pytest

from arti import Artifact, Graph, GraphSnapshot, PartitionKey, Statistic, Type, producer
from arti.formats.json import JSON
from arti.partitions import DateField
from arti.producers import one_to_one
from arti.statistics import (
    DistinctCount,
    FieldStatistic,
    Max,
    NullCount,
    Ratio,
    RowCount,
    register_implementation,
)
from arti.storage.local import LocalFile
from arti.thresholds import Range, Threshold, ThresholdError
from arti.types import Collection, Date, Float64, Int64, Struct
from arti.views.python import List

ROW = Struct(fields={"id": Int64(), "day": Date(), "amount": Float64(nullable=True)})


class Transactions(Artifact):
    type: Type = Collection(element=ROW, partition_by=("day",))
    format: JSON = JSON()


class Row(TypedDict):
    id: int
    amount: float | None


class Total(Artifact):
    type: Type = Float64()
    format: JSON = JSON()


class Median(FieldStatistic):
    """A Statistic without any implementations."""

    def value_type(self, artifact_type: Type) -> Type:
        return Float64(nullable=True)


class FlakyRowCount(RowCount):
    """A RowCount that fails while FAIL is set, eg: due to a bug or a transient error."""


FAIL = {"flaky": False}


@register_implementation
def flaky_row_count(statistic: FlakyRowCount, data: list[Any], view: List) -> int:
    if FAIL["flaky"]:
        raise RuntimeError("Failed to count")
    return len(data)


CALLS = {"clean": 0, "total": 0}


@producer(map=one_to_one("raw"))
def clean(
    raw: Annotated[list[Row], Transactions],
) -> Annotated[list[Row], Transactions]:
    CALLS["clean"] += 1
    return [row for row in raw if row["amount"] is not None]


@producer()
def total(transactions: Annotated[list[Row], Transactions]) -> Annotated[float, Total]:
    CALLS["total"] += 1
    return float(sum(row["amount"] or 0 for row in transactions))


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.update(clean=0, total=0)
    FAIL.update(flaky=False)


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir()
    # Written outside of Artigraph, so the raw Statistics are computed at build time. The partition
    # field (day) is only in the path, not the rows.
    for day, amounts in {"2026-01-01": [1.5, None, 3.0], "2026-01-02": [10.0]}.items():
        rows = [{"id": i, "amount": amount} for i, amount in enumerate(amounts)]
        (raw / f"{day}.json").write_text(json.dumps(rows))
    return raw


def make_graph(
    tmp_path: Path,
    raw_dir: Path,
    *,
    raw_statistics: tuple[Statistic, ...] = (RowCount(), NullCount(field="amount")),
    raw_thresholds: tuple[Threshold, ...] = (),
    clean_statistics: tuple[Statistic, ...] = (RowCount(), Max(field="amount")),
    clean_thresholds: tuple[Threshold, ...] = (),
) -> Graph:
    with Graph(name="test") as g:
        g.artifacts.raw = Transactions(
            storage=LocalFile(path=str(raw_dir / "{day.iso}.json")),
            statistics=raw_statistics,
            thresholds=raw_thresholds,
        )
        g.artifacts.clean = clean(raw=g.artifacts.raw).out(
            Transactions(
                storage=LocalFile.rooted_at(tmp_path / "produced"),
                statistics=clean_statistics,
                thresholds=clean_thresholds,
            )
        )
        g.artifacts.total = total(transactions=g.artifacts.clean).out(
            Total(storage=LocalFile.rooted_at(tmp_path / "produced"))
        )
    return g


def by_day(snapshot: GraphSnapshot, artifact: Artifact) -> dict[str, dict[Statistic, Any]]:
    return {
        cast(DateField, partition.partition_key["day"]).iso: values
        for partition, values in snapshot.read_statistics(artifact).items()
    }


def test_build_records_statistics(tmp_path: Path, raw_dir: Path) -> None:
    snapshot = make_graph(tmp_path, raw_dir).build()

    assert by_day(snapshot, snapshot.artifacts.raw) == {
        "2026-01-01": {RowCount(): 3, NullCount(field="amount"): 1},
        "2026-01-02": {RowCount(): 1, NullCount(field="amount"): 0},
    }
    assert by_day(snapshot, snapshot.artifacts.clean) == {
        "2026-01-01": {RowCount(): 2, Max(field="amount"): 3.0},
        "2026-01-02": {RowCount(): 1, Max(field="amount"): 10.0},
    }
    assert snapshot.read_statistics(snapshot.artifacts.total) == {
        partition: {} for partition in snapshot.read_statistics(snapshot.artifacts.total)
    }
    assert snapshot.read(snapshot.artifacts.total, annotation=float) == 14.5


def test_failed_thresholds_stop_the_build(tmp_path: Path, raw_dir: Path) -> None:
    strict = (Range(statistic=RowCount(), min=2), Range(statistic=Max(field="amount"), max=5))
    with pytest.raises(ThresholdError) as exc_info:
        make_graph(tmp_path, raw_dir, clean_thresholds=strict).build()
    error = exc_info.value
    assert error.artifact_key == "clean"
    assert {(failure.threshold, failure.value) for failure in error.failures} == {
        (strict[0], 1),
        (strict[1], 10.0),
    }
    assert "`clean` failed 2 Threshold check(s):" in str(error)
    # The downstream Producer never read the failing partitions.
    assert CALLS == {"clean": 2, "total": 0}

    # Statistics are facts and Thresholds are judgements: revising the Thresholds rechecks the
    # recorded Statistics without rebuilding the data.
    relaxed = (Range(statistic=RowCount(), min=1), Range(statistic=Max(field="amount"), max=10))
    snapshot = make_graph(tmp_path, raw_dir, clean_thresholds=relaxed).build()
    assert CALLS == {"clean": 2, "total": 1}
    assert snapshot.read(snapshot.artifacts.total, annotation=float) == 14.5


def test_raw_thresholds(tmp_path: Path, raw_dir: Path) -> None:
    # A compound judgement checks a derived Statistic, here the fraction of null amounts.
    null_fraction = Ratio(numerator=NullCount(field="amount"), denominator=RowCount())
    g = make_graph(tmp_path, raw_dir, raw_thresholds=(Range(statistic=null_fraction, max=0.25),))
    with pytest.raises(
        ThresholdError, match=r"`raw` failed 1 Threshold check\(s\):\n- .*2026, 1, 1.* got 0.33"
    ):
        g.build()
    assert CALLS == {"clean": 0, "total": 0}


def test_threshold_overrides(
    tmp_path: Path, raw_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    strict = Range(statistic=Max(field="amount"), max=5)
    g = make_graph(tmp_path, raw_dir, clean_thresholds=(strict,))
    with pytest.raises(ThresholdError, match=r"Graph\.override_threshold") as exc_info:
        g.build()
    (failure,) = exc_info.value.failures

    # A human accepts the failing partition, so the build continues.
    g.override_threshold(
        g.artifacts.clean, failure.partition, failure.threshold, reason="A big purchase"
    )
    snapshot = g.build()
    assert snapshot.read(snapshot.artifacts.total, annotation=float) == 14.5
    assert CALLS == {"clean": 2, "total": 1}
    assert any(
        "Accepting the overridden `clean` failure (A big purchase)" in message
        for message in caplog.messages
    )

    # Overrides apply to the exact Threshold, so revising it requires a new decision.
    stricter = Range(statistic=Max(field="amount"), max=4)
    with pytest.raises(ThresholdError):
        make_graph(tmp_path, raw_dir, clean_thresholds=(stricter,)).build()


def test_revising_statistics_and_thresholds_does_not_rebuild(tmp_path: Path, raw_dir: Path) -> None:
    def clean_partitions(snapshot: GraphSnapshot) -> set[Any]:
        with snapshot.backend.connect() as connection:
            return set(
                connection.read_snapshot_partitions(snapshot, "clean", snapshot.artifacts.clean)
            )

    first = make_graph(tmp_path, raw_dir).build()
    revised = make_graph(
        tmp_path,
        raw_dir,
        raw_statistics=(DistinctCount(field="id"),),
        raw_thresholds=(Range(statistic=RowCount(), min=1),),
        clean_statistics=(NullCount(field="id"),),
        clean_thresholds=(Range(statistic=Max(field="amount"), max=10),),
    ).build()
    # The Graph (and snapshot) identity includes the quality metadata...
    assert revised.graph.fingerprint != first.graph.fingerprint
    assert revised.fingerprint != first.fingerprint
    # ...but Producer input fingerprints (and thus Storage paths) only depend on the Producer's
    # name and version and its input partitions' contents, so nothing is rebuilt.
    assert clean_partitions(revised) == clean_partitions(first)
    assert CALLS == {"clean": 2, "total": 1}


def test_failed_statistics_dont_lose_produced_data(tmp_path: Path, raw_dir: Path) -> None:
    FAIL["flaky"] = True
    g = make_graph(tmp_path, raw_dir, clean_thresholds=(Range(statistic=FlakyRowCount(), min=1),))
    with pytest.raises(RuntimeError, match="Failed to count"):
        g.build()
    # The first output partition was recorded before its Statistics failed, but not checked (nor
    # read downstream).
    assert CALLS == {"clean": 1, "total": 0}

    FAIL["flaky"] = False
    snapshot = g.build()
    # The recorded partition is reused, with its Statistics computed by reading it back.
    assert CALLS == {"clean": 2, "total": 1}
    assert by_day(snapshot, snapshot.artifacts.clean) == {
        "2026-01-01": {RowCount(): 2, Max(field="amount"): 3.0, FlakyRowCount(): 2},
        "2026-01-02": {RowCount(): 1, Max(field="amount"): 10.0, FlakyRowCount(): 1},
    }


def test_added_statistics_are_computed_from_existing_partitions(
    tmp_path: Path, raw_dir: Path
) -> None:
    make_graph(tmp_path, raw_dir, clean_statistics=()).build()
    snapshot = make_graph(tmp_path, raw_dir, clean_statistics=(DistinctCount(field="id"),)).build()
    # The existing partitions are reused, but read with the Producer's View for the new Statistic.
    assert CALLS == {"clean": 2, "total": 1}
    assert by_day(snapshot, snapshot.artifacts.clean) == {
        "2026-01-01": {DistinctCount(field="id"): 2},
        "2026-01-02": {DistinctCount(field="id"): 1},
    }


def test_raw_statistics_need_readable_fields(tmp_path: Path, raw_dir: Path) -> None:
    # The day is only in the file path, so Statistics of it can't be computed from the rows.
    g = make_graph(tmp_path, raw_dir, raw_statistics=(Max(field="day"),))
    with pytest.raises(KeyError, match="day"):
        g.build()


def test_write_records_statistics(tmp_path: Path) -> None:
    with Graph(name="write") as g:
        g.artifacts.raw = Transactions(
            storage=LocalFile(path=str(tmp_path / "{day.iso}.json")),
            statistics=(RowCount(), Max(field="amount")),
        )
    key = PartitionKey(day=DateField(value=date(2026, 1, 1)))
    rows = [{"id": 1, "amount": 1.0}, {"id": 2, "amount": None}]
    partition = g.write(rows, artifact=g.artifacts.raw, partition_key=key)
    with g.backend.connect() as connection:
        assert connection.read_partition_statistics(g.artifacts.raw, (partition,)) == {
            partition: {RowCount(): 2, Max(field="amount"): 1.0}
        }

    # The partition is recorded before computing Statistics, so a failure doesn't lose it. The
    # missing Statistics are computed again by the next build (or read as missing until then).
    second = PartitionKey(day=DateField(value=date(2026, 1, 2)))
    with pytest.raises(KeyError, match="amount"):
        g.write([{"id": 1}], artifact=g.artifacts.raw, partition_key=second)
    assert (tmp_path / "2026-01-02.json").exists()
    with g.backend.connect() as connection:
        (written,) = (
            p
            for p in connection.read_artifact_partitions(g.artifacts.raw)
            if p.partition_key == second
        )
        assert connection.read_partition_statistics(g.artifacts.raw, (written,)) == {written: {}}


def test_statistics_require_an_implementation(tmp_path: Path, raw_dir: Path) -> None:
    match = "No `Statistic` implementation found for: statistic=Median"
    # Produced Artifacts are checked when the Graph is defined...
    with pytest.raises(ValueError, match=match):
        make_graph(tmp_path, raw_dir, clean_statistics=(Median(field="amount"),))
    # ...and raw Artifacts before writing.
    with Graph(name="write") as g:
        g.artifacts.raw = Transactions(
            storage=LocalFile(path=str(tmp_path / "{day.iso}.json")),
            statistics=(Median(field="amount"),),
        )
    key = PartitionKey(day=DateField(value=date(2026, 1, 1)))
    with pytest.raises(ValueError, match=match):
        g.write([{"id": 1, "amount": 1.0}], artifact=g.artifacts.raw, partition_key=key)
    assert not (tmp_path / "2026-01-01.json").exists()


def test_statistics_view(tmp_path: Path, raw_dir: Path) -> None:
    g = make_graph(tmp_path, raw_dir)
    assert type(g.statistics_view(g.artifacts.raw)).__name__ == "List"
    assert g.statistics_view(g.artifacts.raw).mode == "READ"
    assert g.statistics_view(g.artifacts.clean).mode == "READ"
