package com.opower.hadoop.hbase.query;

/**
 * A default implementation of the {@link QueryPlanner} that is the main entry-point for
 * doing queries against HBase.
 *
 * @author jeff@opower.com
 */
public class DefaultQueryPlanner implements QueryPlanner {
    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the input is unparsable
     */
    @Override
    public Query parse(String query) {
        QueryBuilder builder = QueryBuilder.parse(query);
        return new Query(this, builder);
    }
}
