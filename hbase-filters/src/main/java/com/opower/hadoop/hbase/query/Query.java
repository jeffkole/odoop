package com.opower.hadoop.hbase.query;

import org.apache.hadoop.hbase.client.ResultScanner;

import java.math.BigDecimal;
import java.io.Closeable;
import java.io.IOException;

/**
 * Encapsulates the configuration for a query that will be run against HBase
 *
 * @author jeff@opower.com
 */
public interface Query extends Closeable {
    void close() throws IOException;

    ResultScanner scan() throws IOException;

    Query setTimestamp(String parameter, long timestamp);

    Query setBytes(String parameter, byte[] value);

    Query setBoolean(String parameter, boolean value);

    Query setBigDecimal(String parameter, BigDecimal value);

    Query setDouble(String parameter, double value);

    Query setFloat(String parameter, float value);

    Query setInt(String parameter, int value);

    Query setLong(String parameter, long value);

    Query setShort(String parameter, short value);

    Query setString(String parameter, String value);

    Query setStringBinary(String parameter, String value);
}
