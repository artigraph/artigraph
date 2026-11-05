"""An Airflow DAG file that builds the executor test Graph (see `tests/arti/executors/graphs.py`)."""

from arti.executors.airflow import make_dag
from tests.arti.executors.graphs import GRAPH_REF

dag = make_dag(GRAPH_REF, dag_id="arti-executor-conformance", schedule=None)
