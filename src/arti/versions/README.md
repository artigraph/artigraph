# Version

A Version is an arbitrary version tag for the Producer, such as SemVer, CalVer, Git SHA, a string, etc. The Version is combined with the Producer’s input Artifacts’ Fingerprints in order to determine what output Artifacts need built. If the Version changes or an input Artifact’s Fingerprint changes, then the output Artifacts will need rebuilt. If the Producer is “partition aware”, this logic is applied on a partition-by-partition basis - the Version is held constant, but the Artifact Fingerprints will be compared per-partition.
