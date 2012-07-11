package com.opower.hadoop.hbase.query;

/**
 * Prepares queries for execution against an HBase cluster
 *
 * @author jeff@opower.com
 */
public interface QueryPlanner {
    /**
     * Parse a query and construct a {@link Query} object that can be used to actually run it against HBase
     *
     * @param query the query to parse and prepare
     * @return a {@link Query} that can be run against HBase
     */
    Query parse(String query);
}
