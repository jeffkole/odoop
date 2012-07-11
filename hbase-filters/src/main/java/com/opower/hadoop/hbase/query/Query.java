package com.opower.hadoop.hbase.query;

import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the configuration for a query that will be run against HBase
 *
 * @author jeff@opower.com
 */
public class Query {
    private final DefaultQueryPlanner queryPlanner;
    private final QueryBuilder queryBuilder;
    private final Map<String, byte[]> parameters = new HashMap<String, byte[]>();
    private final Map<String, Long> timestamps = new HashMap<String, Long>();

    Query(DefaultQueryPlanner queryPlanner, QueryBuilder queryBuilder) {
        this.queryPlanner = queryPlanner;
        this.queryBuilder = queryBuilder;
    }

    public ResultScanner scan() {
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
}
