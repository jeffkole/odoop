package com.opower.hadoop.hbase.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

/**
 * A default implementation of the {@link QueryPlanner} that is the main entry-point for
 * running queries against HBase.
 *
 * @author jeff@opower.com
 */
public class DefaultQueryPlanner implements QueryPlanner {
    private static final Log LOG = LogFactory.getLog(DefaultQueryPlanner.class);

    private final HTablePool hTablePool;

    public DefaultQueryPlanner(HTablePool hTablePool) {
        this.hTablePool = hTablePool;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the input is unparsable
     */
    @Override
    public Query parse(String query) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Parsing '%s'", query));
        }
        QueryBuilder builder = QueryBuilder.parse(query);
        return new DefaultQuery(this, builder);
    }

    /**
     * Closes all resources associated with this planner
     */
    @Override
    public void close() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Closing HTablePool");
        }
        this.hTablePool.close();
    }

    /**
     * Get an {@link HTableInterface} from the internal table pool.  This method is meant to be called
     * by a {@link Query}.
     *
     * @param tableName the name of a table to get from the pool
     * @return an instance from the internal table pool
     */
    HTableInterface getTable(String tableName) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Getting table from pool named '%s'", tableName));
        }
        HTableInterface table = this.hTablePool.getTable(tableName);
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Table named %s from pool is %s", tableName, table));
        }
        return table;
    }

    /**
     * Return the table to the pool.  This method is meant to be called by a {@link Query}
     *
     * @param table the table to return to the pool
     */
    void putTable(HTableInterface table) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Returning table to pool %s", table));
        }
        this.hTablePool.putTable(table);
    }
}
