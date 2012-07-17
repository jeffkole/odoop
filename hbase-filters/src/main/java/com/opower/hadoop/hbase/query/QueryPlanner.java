package com.opower.hadoop.hbase.query;

import java.io.Closeable;
import java.io.IOException;

/**
 * Prepares queries for execution against an HBase cluster
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

    /**
     * Release any resources associated with this planner
     */
    void close() throws IOException;
}
