package com.opower.hadoop.hbase.query;

import java.io.Closeable;

/**
 * Prepares queries for execution against an HBase cluster.
 * </p><p>
 * Example usage:
 * </p>
 * <pre>
        Configuration configuration = HBaseConfiguration.create();
        HTablePool pool = new HTablePool(configuration, maxPoolSize);
        QueryPlanner planner = new DefaultQueryPlanner(pool);
        Query query = null;
        ResultScanner scanner = null;
        try {
            query = planner.parse("scan all versions of d:purchases between {start} and {stop} " +
                "from customers where rowkey = {id}");
            query.setTimestamp("start", new DateMidnight("2012-01-01").toMillis())
                .setTimestamp("stop", new DateMidnight("2012-02-01").toMillis())
                .setString("id", customerId);
            scanner = query.scan();
            for (Result result : scanner) {
                KeyValue[] kvs = result.raw();
            }
        }
        finally {
            closeQuietly(scanner);
            closeQuietly(query);
            closeQuietly(planenr);
            closeQuietly(pool);
        }
 * </pre>
 *
 * @author jeff@opower.com
 */
public interface QueryPlanner extends Closeable {
    /**
     * Parse a query and construct a {@link Query} object that can be used to actually run it against HBase
     *
     * @param query the query to parse and prepare
     * @return a {@link Query} that can be run against HBase
     */
    Query parse(String query);
}
