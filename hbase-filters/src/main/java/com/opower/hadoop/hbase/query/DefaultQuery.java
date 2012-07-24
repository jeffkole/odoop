package com.opower.hadoop.hbase.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of a {@link Query}, which collaborates with the {@link DefaultQueryPlanner}
 *
 * @author jeff@opower.com
 */
public class DefaultQuery implements Query {
    private static final Log LOG = LogFactory.getLog(DefaultQuery.class);

    private final DefaultQueryPlanner queryPlanner;
    private final QueryBuilder queryBuilder;
    private final Map<String, byte[]> parameters = new HashMap<String, byte[]>();
    private final Map<String, Long> timestamps = new HashMap<String, Long>();

    private HTableInterface hTable;

    DefaultQuery(DefaultQueryPlanner queryPlanner, QueryBuilder queryBuilder) {
        this.queryPlanner = queryPlanner;
        this.queryBuilder = queryBuilder;
    }

    public void close() {
        if (this.hTable != null) {
            this.queryPlanner.putTable(this.hTable);
        }
        this.hTable = null;
    }

    /**
     * Plan and run the query, resulting in a scan operation on HBase
     *
     * {@inheritDoc}
     */
    @Override
    public ResultScanner scan() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Planning scan with parameters (%s) and timestamps (%s)",
                        this.parameters, this.timestamps));
        }
        Scan scan = this.queryBuilder.planScan(this.parameters, this.timestamps);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Scan: %s, filter: %s", scan, inspectFilter(scan.getFilter())));
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Getting table named '%s'", this.queryBuilder.getTableName()));
        }
        this.hTable = this.queryPlanner.getTable(this.queryBuilder.getTableName());
        return this.hTable.getScanner(scan);
    }

    public Query setTimestamp(String parameter, long timestamp) {
        this.timestamps.put(parameter, timestamp);
        return this;
    }

    public Query setBytes(String parameter, byte[] value) {
        this.parameters.put(parameter, value);
        return this;
    }

    public Query setBoolean(String parameter, boolean value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setBigDecimal(String parameter, BigDecimal value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setDouble(String parameter, double value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setFloat(String parameter, float value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setInt(String parameter, int value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setLong(String parameter, long value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setShort(String parameter, short value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setString(String parameter, String value) {
        this.parameters.put(parameter, Bytes.toBytes(value));
        return this;
    }

    public Query setStringBinary(String parameter, String value) {
        this.parameters.put(parameter, Bytes.toBytesBinary(value));
        return this;
    }

    private static String inspectFilter(Filter filter) {
        if (filter == null) {
            return "null";
        }
        if (filter instanceof FilterList) {
            return inspectFilter((FilterList)filter);
        }
        return filter.toString();
    }

    private static String inspectFilter(FilterList filterList) {
        StringBuilder buf = new StringBuilder();
        for (Filter filter : filterList.getFilters()) {
            buf.append(inspectFilter(filter)).append(", ");
        }
        return "FilterList(" + filterList.getOperator() + ") {" + buf + "}";
    }
}
