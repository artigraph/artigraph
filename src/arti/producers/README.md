# Producer

A Producer is a task that builds one or more Artifacts.

A Producer is a computation using a set of input Artifacts to produce a set of output Artifacts. A Producer operates on Views of the Artifacts, with the framework handling the reading and writing of the data. Each Producer has a version which is used to identify when the output Artifacts need rebuilt, in addition to the input Artifacts changing. For partitioned Artifacts, Producers may define a input -> output partition mapping and the framework will handle building the partitions incrementally. Producers may have dependencies on resources, which are connections to external services, such as a Dask cluster, BigQuery, etc. Arbitrary hooks may be added to the Producer to add custom logic, such as instrumentation, that will run at certain points, such as before or after building Artifacts.
