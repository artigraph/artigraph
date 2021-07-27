from arti.annotations.core import Annotation
from arti.artifacts.core import Artifact
from arti.graphs.core import Graph
from arti.producers.core import Producer
from arti.types.core import Date, Int32, Int64, String, Struct, Timestamp


class Vendor(Annotation):
    def __init__(self, vendor: str) -> None:
        super().__init__()
        self.vendor = vendor


class Traces(Artifact):
    type = Struct(
        {"uid": Int64(), "lat": Int64(), "lng": Int64(), "timestamp": Timestamp("millisecond")}
    )


class ODs(Artifact):
    type = Struct(
        {
            "origin": String(),
            "destination": String(),
            "date": Date(),
            "hour": Int32(description="Localized hour"),
            "count": Int64(),
        }
    )


class TracesToODs(Producer):
    # This currently takes the Artifact directly, but eventually it'll take a "view" of the artifact (eg: pd.DataFrame)
    # and optionally a "field slice" (eg: [lat, lng, timestamp]).
    @classmethod
    def build(cls, traces: Traces) -> ODs:
        ...


with Graph("ODs") as graph:
    a = graph.artifacts

    a.traces = Traces(annotations=[Vendor("Acme")])
    a.ods = TracesToODs(traces=a.traces)
