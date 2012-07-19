package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.opower.hadoop.hbase.test.HBaseTestRunner;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

/**
 * Tests the {@link ColumnVersionTimerangeFilter} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
@RunWith(HBaseTestRunner.class)
public class IntTestColumnVersionTimerangeFilter {
    private static final String TABLE_NAME = IntTestColumnVersionTimerangeFilter.class.getName();
    private static final byte[] FAMILY = Bytes.toBytes("d");
    // row, family, qualifier, timestamp, value
    private static final Object[][] RECORDS = new Object[][] {
        { "row-A", "d", "qual-A", 1L, "value-A1" },
        { "row-A", "d", "qual-A", 2L, "value-A2" },
        { "row-A", "d", "qual-A", 3L, "value-A3" },
        { "row-A", "d", "qual-A", 4L, "value-A4" },
        { "row-A", "d", "qual-A", 5L, "value-A5" },

        { "row-A", "d", "qual-B", 2L, "value-B2" },
        { "row-A", "d", "qual-B", 3L, "value-B3" },
        { "row-A", "d", "qual-B", 4L, "value-B4" },
        { "row-A", "d", "qual-B", 5L, "value-B5" },
        { "row-A", "d", "qual-B", 6L, "value-B6" },

        { "row-B", "d", "qual-A", 2L, "value-A2" },
        { "row-B", "d", "qual-A", 3L, "value-A3" },
        { "row-B", "d", "qual-A", 4L, "value-A4" },
        { "row-B", "d", "qual-A", 5L, "value-A5" },
        { "row-B", "d", "qual-A", 6L, "value-A6" },

        { "row-B", "d", "qual-B", 3L, "value-B3" },
        { "row-B", "d", "qual-B", 4L, "value-B4" },
        { "row-B", "d", "qual-B", 5L, "value-B5" },
        { "row-B", "d", "qual-B", 6L, "value-B6" },
        { "row-B", "d", "qual-B", 7L, "value-B7" },
    };

    private static HBaseTestingUtility hbaseTestingUtility;
    private static HTable table;

    @BeforeClass
    public static void setUpClass() throws Exception {
        byte[] tableName = Bytes.toBytes(TABLE_NAME);
        table = hbaseTestingUtility.createTable(tableName, FAMILY, 100);
        table.setAutoFlush(false);
        for (Object[] record : RECORDS) {
            int i = 0;
            byte[] rowKey = Bytes.toBytes((String)record[i++]);
            byte[] family = Bytes.toBytes((String)record[i++]);
            byte[] qualifier = Bytes.toBytes((String)record[i++]);
            long ts = (Long)record[i++];
            byte[] value = Bytes.toBytes((String)record[i++]);
            Put put = new Put(rowKey);
            put.add(family, qualifier, ts, value);
            table.put(put);
        }
        table.flushCommits();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hbaseTestingUtility.deleteTable(Bytes.toBytes(TABLE_NAME));
    }

    @Test
    public void testMaxVersions() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "qual-A", 5L, "value-A5" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-A", 3L, "value-A3" },
            { "row-B", "d", "qual-A", 6L, "value-A6" },
            { "row-B", "d", "qual-A", 5L, "value-A5" },
            { "row-B", "d", "qual-A", 4L, "value-A4" },
        };
        runFilterAssertions(new ColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual-A"), 3),
                expectedResults, 2);
    }

    private void runFilterAssertions(Filter filter, Object[][] expectedResults, int expectedNumRows) throws Exception {
        Scan scan = new Scan();
        scan.addFamily(FAMILY);
        scan.setFilter(filter);
        scan.setMaxVersions();
        int numRows = 0;
        int numKeyValues = 0;
        int resultIndex = 0;
        for (Result result : table.getScanner(scan)) {
            KeyValue[] keyValues = result.raw();
            for (KeyValue keyValue : keyValues) {
                String expectedRowKey    = (String)expectedResults[resultIndex][0];
                String expectedFamily    = (String)expectedResults[resultIndex][1];
                String expectedQualifier = (String)expectedResults[resultIndex][2];
                long expectedTimestamp   = (Long)expectedResults[resultIndex][3];
                String expectedValue     = (String)expectedResults[resultIndex][4];
                assertThat("row key matches",   Bytes.toString(keyValue.getRow()),       is(expectedRowKey));
                assertThat("family matches",    Bytes.toString(keyValue.getFamily()),    is(expectedFamily));
                assertThat("qualifier matches", Bytes.toString(keyValue.getQualifier()), is(expectedQualifier));
                assertThat("timestamp matches", keyValue.getTimestamp(),                 is(expectedTimestamp));
                assertThat("value matches",     Bytes.toString(keyValue.getValue()),     is(expectedValue));
                resultIndex++;
                numKeyValues++;
            }
            numRows++;
        }
        assertThat("number of rows matches", numRows, is(expectedNumRows));
        assertThat("number of keyvalue matches", numKeyValues, is(expectedResults.length));
    }

    @Test
    public void testTimeranges() throws Exception {

    }
}
