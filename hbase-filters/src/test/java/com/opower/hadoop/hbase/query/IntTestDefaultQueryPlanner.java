package com.opower.hadoop.hbase.query;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import com.opower.hadoop.hbase.test.HBaseTestRunner;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

/**
 * Tests the {@link DefaultQueryPlanner} and {@link DefaultQuery} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
@RunWith(HBaseTestRunner.class)
public class IntTestDefaultQueryPlanner {
    private static final String TABLE_NAME = IntTestDefaultQueryPlanner.class.getName();
    private static final String FAMILY_A = "familyA";
    private static final String FAMILY_B = "familyB";
    private static final String FAMILY_C = "familyC";
    private static final byte[][] FAMILIES = new byte[][] {
        Bytes.toBytes(FAMILY_A),
        Bytes.toBytes(FAMILY_B),
        Bytes.toBytes(FAMILY_C),
    };
    private static final String[] ROWS = new String[] {
        "apple",
        "banana",
        "orange",
        "cantaloupe",
        "strawberry",
        "nectarine",
        "apricot",
        "peach",
        "watermelon",
        "cherry",
    };
    // qualifier, num versions, timestamp start, timestamp interval
    private static final Object[][] QUALIFIERS = new Object[][] {
        { "oneValueA",     1,    100L,    0, },
        { "oneBalueB",     1,  10000L,    0, },
        { "fiveValues",    5,    100L,  100, },
        { "tenValuesA",   10,   1000L,   10, },
        { "tenValuesB",   10,   1000L, 1000, },
    };

    private static HBaseTestingUtility hbaseTestingUtility;
    private static HTable table;
    private static HTableInterfaceFactory tableFactory;
    private static int numKeyValues;

    private QueryPlanner queryPlanner;

    private static final class StaticTableFactory implements HTableInterfaceFactory {
        private HTable table;
        private StaticTableFactory(HTable table) {
            this.table = table;
        }

        public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
            return table;
        }

        public void releaseHTableInterface(HTableInterface table) {}
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        byte[] tableName = Bytes.toBytes(TABLE_NAME);
        table = hbaseTestingUtility.createTable(tableName, FAMILIES, 100);
        tableFactory = new StaticTableFactory(table);

        // Now load a bunch of data into the table
        numKeyValues = 0;
        table.setAutoFlush(false);
        for (byte[] family : FAMILIES) {
            for (String row : ROWS) {
                byte[] rowKey = Bytes.toBytes(row);
                Put put = new Put(rowKey);
                for (Object[] qualifiers : QUALIFIERS) {
                    int i = 0;
                    byte[] qualifier = Bytes.toBytes((String)qualifiers[i++]);
                    int numVersions = (Integer)qualifiers[i++];
                    long start = (Long)qualifiers[i++];
                    int interval = (Integer)qualifiers[i++];
                    for (int v = 0; v < numVersions; v++) {
                        byte[] value = Bytes.add(rowKey, Bytes.toBytes(Integer.toString(v)));
                        put.add(family, qualifier, start + (v * interval), value);
                        numKeyValues++;
                    }
                }
                table.put(put);
            }
        }
        table.flushCommits();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hbaseTestingUtility.deleteTable(Bytes.toBytes(TABLE_NAME));
    }

    @Before
    public void setUp() {
        this.queryPlanner = new DefaultQueryPlanner(new HTablePool(hbaseTestingUtility.getConfiguration(), 1, tableFactory));
    }

    @After
    public void tearDown() throws Exception {
        this.queryPlanner.close();
    }

    @Test
    public void testSimple() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME);
        assertThat("query is not null", query, is(notNullValue()));
        int rowCount = 0;
        int keyValueCount = 0;
        ResultScanner scanner = null;
        try {
            scanner = query.scan();
            assertThat("scanner is not null", scanner, is(notNullValue()));
            Result result = null;
            while ((result = scanner.next()) != null) {
                rowCount++;
                for (byte[] family : FAMILIES) {
                    for (Object[] qualifiers : QUALIFIERS) {
                        byte[] qualifier = Bytes.toBytes((String)qualifiers[0]);
                        List<KeyValue> keyValues = result.getColumn(family, qualifier);
                        keyValueCount += keyValues.size();
                    }
                }
            }
            assertThat("row count is correct", rowCount, equalTo(ROWS.length));
            // we expect only one version per qualifier
            assertThat("key value count is correct", keyValueCount,
                    equalTo(1 * QUALIFIERS.length * ROWS.length * FAMILIES.length));
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
            if (query != null) {
                query.close();
            }
        }
    }
}
