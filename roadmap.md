---
layout: main
title: Roadmap
---

Potential features and improvements include in no particular order

* Multiplexer for map or reduce inputs so that the same data set can be
  processed in parallel (this may be difficult to manage when jobs need to be
  retried, etc.)
* Migrate `HKey` from internal library
* Migrate `HBaseTemplate` from internal library
* Add `hbase shell` support for query planner
* Add column constraints and boolean logic
* Skip ScalaObject along with hbase-filters
* Add unit tests to ensure garbage collection of deployed filters
* Add ability to register multiple filter classes with `DeployedFilterManager`
  and clean up at client shutdown
* Use distributed cache for jar shipping, so that HDFS does not get bogged down
  when the Scan starts

Documentation tasks include in no particular order

* Performance metrics
* Document code intent of query planner (i.e., write one line of SQL-like stuff
  instead of amassing multiple layers of FilterList and other filters)
