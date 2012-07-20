package com.opower.hadoop.hbase.query;

import org.apache.hadoop.hbase.client.ResultScanner;

import java.math.BigDecimal;
import java.io.Closeable;
import java.io.IOException;

/**
 * Encapsulates the configuration for and exposes the behavior for
 * a query that will be run against HBase
 *
 * @author jeff@opower.com
 */
public interface Query extends Closeable {
    /**
     * Run a scan query against HBase
     *
     * @return the results of the scan
     * @throws IOException in case of RPC badness
     */
    ResultScanner scan() throws IOException;

    /**
     * Set a timestamp parameter that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param timestamp the timestamp value to use for the parameter
     * @return this, so that you can chain
     */
    Query setTimestamp(String parameter, long timestamp);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setBytes(String parameter, byte[] value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setBoolean(String parameter, boolean value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setBigDecimal(String parameter, BigDecimal value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setDouble(String parameter, double value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setFloat(String parameter, float value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setInt(String parameter, int value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setLong(String parameter, long value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setShort(String parameter, short value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setString(String parameter, String value);

    /**
     * Set a parameter value that corresponds to a named parameter
     * in the raw query
     *
     * @param parameter the name of a parameter in the query
     * @param value the value to use for the parameter
     * @return this, so that you can chain
     */
    Query setStringBinary(String parameter, String value);
}
