---
layout: doc
title: Deployed Filter
---

The deployed filter system is a mechanism for sending HBase filters from the client to the server to be run for a single
operation.  This will allow you to develop new filters for HBase without having to restart your region servers each time
you want to redeploy the filters.

As an example, let's say you have the need to scan over randomly selected rows from a table in HBase.  HBase does not ship
with `RandomRowFilter`, so you code it up yourself.  If you had already deployed the deployed filter system to HBase, you
could simply write some code in your client to start using your new filter.  And if you discover a bug in your filter,
instead of redeploying it and restarting your region servers, you can just rerun your client with the bug fixed, and the new
version of your filter will be used.

### Example Usage

    Configuration configuration = HBaseConfiguration.create();
    DeployedFilterManager filterManager = new DeployedFilterManager(configuration);
    DeployedFilter filter = filterManager.deployFilter(new RandomRowFilter());

    Scan scan = new Scan();
    scan.setFilter(filter);

    try {
        HTable table = new HTable(configuration, tableName);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            KeyValue[] kvs = result.raw();
        }
        scanner.close();
    }
    finally {
        filterManager.undeployFilter(filter);
        HConnectionManager.deleteConnection(configuration, true);
    }

### Installation

To start using the deployed filter system, you need to push a jar out to your region servers and add it to their classpath.
The jar you need is called `deployed-filter-*-bin.jar` and is assembled under the `hbase-filters` module when you run
`mvn install`.  Look in the `target` directory under `hbase-filters` after you run Maven.  You can add it to your region
server classpath by setting the `HBASE_CLASSPATH` environment variable to include a reference to it.

The deployed filter system acts as an HBase metrics source so you can monitor its behavior in your cluster.  To enable metrics
collection from it, you need to add a line to your `hadoop-metrics.properties` file on your region servers similar to this:

    extensions.class=org.apache.hadoop.metrics.spi.NoEmitMetricsContext

The deployed filter system adds metrics to the `extensions` group.  Set its metrics class to correspond with the metrics
collection system you use.
