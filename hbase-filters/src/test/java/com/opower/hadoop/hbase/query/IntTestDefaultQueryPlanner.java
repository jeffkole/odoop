package com.opower.hadoop.hbase.query;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

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
    private static final String TABLE_NAME = IntTestDefaultQueryPlanner.class.getName().toLowerCase();
    private static final String FAMILY_A = "family-a";
    private static final String FAMILY_B = "family-b";
    private static final String FAMILY_C = "family-c";

    private static HBaseTestingUtility hbaseTestingUtility;
    private static HTable table;
    private static HTableInterfaceFactory tableFactory;

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
        byte[][] families = new byte[][] {
            Bytes.toBytes(FAMILY_A),
            Bytes.toBytes(FAMILY_B),
            Bytes.toBytes(FAMILY_C),
        };
        table = hbaseTestingUtility.createTable(tableName, families, 100);
        tableFactory = new StaticTableFactory(table);

        // Now load a bunch of data into the table
        String[] rows = new String[] {
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
        Object[][] columns = new Object[][] {
            { "one-value-a",     1,    100L,    0, },
            { "one-value-b",     1,  10000L,    0, },
            { "five-values",     5,    100L,  100, },
            { "ten-values-a",   10,   1000L,   10, },
            { "ten-values-b",   10,   1000L, 1000, },
        };
        table.setAutoFlush(false);
        for (byte[] family : families) {
            for (String row : rows) {
                byte[] rowKey = Bytes.toBytes(row);
                Put put = new Put(rowKey);
                for (Object[] column : columns) {
                    int i = 0;
                    byte[] qualifier = Bytes.toBytes((String)column[i++]);
                    int numVersions = (Integer)column[i++];
                    long start = (Long)column[i++];
                    int interval = (Integer)column[i++];
                    for (int v = 0; v < numVersions; v++) {
                        byte[] value = Bytes.add(rowKey, Bytes.toBytes(Integer.toString(v)));
                        put.add(family, qualifier, start + (v * interval), value);
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
        ResultScanner scanner = null;
        try {
            scanner = query.scan();
            assertThat("scanner is not null", scanner, is(notNullValue()));
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
