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
        "apricot",
        "banana",
        "cantaloupe",
        "cherry",
        "nectarine",
        "orange",
        "peach",
        "strawberry",
        "watermelon",
    };
    // qualifier, num versions, timestamp start, timestamp interval
    private static final Object[][] QUALIFIERS = new Object[][] {
        { "fiveValues",    5,    100L,  100, },
        { "oneValueA",     1,    100L,    0, },
        { "oneValueB",     1,  10000L,    0, },
        { "tenValuesA",   10,   1000L,   10, },
        { "tenValuesB",   10,   1000L, 1000, },
    };

    private static HBaseTestingUtility hbaseTestingUtility;
    private static HTable table;
    private static HTableInterfaceFactory tableFactory;
    private static int numKeyValues;

    private QueryPlanner queryPlanner;

    // TODO: Move this to someplace more common, perhaps odoop-test
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

    /**
     * Returns an array similar to this (if "nectarine" were the only row passed in)
        Object[][] expectedResults = new Object[][] {
            { "nectarine", "familyA", "fiveValues",   500L, "nectarine-fiveValues-4" },
            { "nectarine", "familyA", "oneValueA",    100L, "nectarine-oneValueA-0" },
            { "nectarine", "familyA", "oneValueB",  10000L, "nectarine-oneValueB-0" },
            { "nectarine", "familyA", "tenValuesA",  1090L, "nectarine-tenValuesA-9" },
            { "nectarine", "familyA", "tenValuesB", 10000L, "nectarine-tenValuesB-9" },

            { "nectarine", "familyB", "fiveValues",   500L, "nectarine-fiveValues-4" },
            { "nectarine", "familyB", "oneValueA",    100L, "nectarine-oneValueA-0" },
            { "nectarine", "familyB", "oneValueB",  10000L, "nectarine-oneValueB-0" },
            { "nectarine", "familyB", "tenValuesA",  1090L, "nectarine-tenValuesA-9" },
            { "nectarine", "familyB", "tenValuesB", 10000L, "nectarine-tenValuesB-9" },

            { "nectarine", "familyC", "fiveValues",   500L, "nectarine-fiveValues-4" },
            { "nectarine", "familyC", "oneValueA",    100L, "nectarine-oneValueA-0" },
            { "nectarine", "familyC", "oneValueB",  10000L, "nectarine-oneValueB-0" },
            { "nectarine", "familyC", "tenValuesA",  1090L, "nectarine-tenValuesA-9" },
            { "nectarine", "familyC", "tenValuesB", 10000L, "nectarine-tenValuesB-9" },
        };
     *
     */
    private static Object[][] makeMostRecentExpectedResults(String... rows) {
        Object[][] expectedResults = new Object[FAMILIES.length * rows.length * QUALIFIERS.length][5];
        int resultIndex = 0;
        for (int r = 0; r < rows.length; r++) {
            String row = rows[r];
            for (int f = 0; f < FAMILIES.length; f++) {
                String family = Bytes.toString(FAMILIES[f]);
                for (int q = 0; q < QUALIFIERS.length; q++) {
                    Object[] qualifiers = QUALIFIERS[q];
                    String qualifier = (String)qualifiers[0];
                    int numVersions = (Integer)qualifiers[1];
                    long start = (Long)qualifiers[2];
                    int interval = (Integer)qualifiers[3];
                    long version = start + ((numVersions - 1) * interval);
                    String value = row + "-" + qualifier + "-" + (numVersions - 1);
                    Object[] expectedResult = new Object[] {
                        row, family, qualifier, version, value
                    };
                    expectedResults[resultIndex++] = expectedResult;
                }
            }
        }
        return expectedResults;
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
                        byte[] value = Bytes.toBytes(row + "-" + qualifiers[0] + "-" + v);
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
    public void testSimpleScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME);
        // we expect only one version per qualifier
        runScanAssertions(query, makeMostRecentExpectedResults(ROWS), ROWS.length);
    }

    @Test
    public void testSingleRowScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "nectarine");
        runScanAssertions(query, makeMostRecentExpectedResults("nectarine"), 1);
    }

    @Test
    public void testLessThanRowScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME + " where rowkey < {id}");
        query.setString("id", "cherry");
        runScanAssertions(query, makeMostRecentExpectedResults("apple", "apricot", "banana", "cantaloupe"), 4);
    }

    @Test
    public void testLessThanEqualRowScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME + " where rowkey <= {id}");
        query.setString("id", "cherry");
        runScanAssertions(query, makeMostRecentExpectedResults("apple", "apricot", "banana", "cantaloupe", "cherry"), 5);
    }

    @Test
    public void testGreaterThanRowScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME + " where rowkey > {id}");
        query.setString("id", "cherry");
        runScanAssertions(query, makeMostRecentExpectedResults("nectarine", "orange", "peach", "strawberry", "watermelon"), 5);
    }

    @Test
    public void testGreaterThanEqualRowScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME + " where rowkey >= {id}");
        query.setString("id", "cherry");
        runScanAssertions(query, makeMostRecentExpectedResults(
                    "cherry", "nectarine", "orange", "peach", "strawberry", "watermelon"), 6);
    }

    @Test
    public void testBetweenRowScan() throws Exception {
        Query query = this.queryPlanner.parse("scan from " + TABLE_NAME + " where rowkey between {low} and {high}");
        query.setString("low", "banana");
        query.setString("high", "orange");
        runScanAssertions(query, makeMostRecentExpectedResults("banana", "cantaloupe", "cherry", "nectarine"), 4);
    }

    @Test
    public void testFamilyAndQualifierScan() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan familyA:oneValueA, familyA:fiveValues from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "apple");
        Object[][] expectedResults = new Object[][] {
            { "apple", "familyA", "fiveValues", 500L, "apple-fiveValues-4" },
            { "apple", "familyA", "oneValueA",  100L, "apple-oneValueA-0" },
        };
        runScanAssertions(query, expectedResults, 1);
    }

    @Test
    public void testMultipleFamiliesAndQualifierScan() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan familyA:tenValuesB, familyB:oneValueA, familyC:fiveValues from " + TABLE_NAME + " where rowkey < {id}");
        query.setString("id", "banana");
        Object[][] expectedResults = new Object[][] {
            { "apple", "familyA", "tenValuesB", 10000L, "apple-tenValuesB-9" },
            { "apple", "familyB", "oneValueA",    100L, "apple-oneValueA-0" },
            { "apple", "familyC", "fiveValues",   500L, "apple-fiveValues-4" },

            { "apricot", "familyA", "tenValuesB", 10000L, "apricot-tenValuesB-9" },
            { "apricot", "familyB", "oneValueA",    100L, "apricot-oneValueA-0" },
            { "apricot", "familyC", "fiveValues",   500L, "apricot-fiveValues-4" },
        };
        runScanAssertions(query, expectedResults, 2);
    }

    @Test
    public void testAllVersionsForColumnsScan() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan all versions of familyA:oneValueA, all versions of familyA:fiveValues " +
                "from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "cherry");
        Object[][] expectedResults = new Object[][] {
            { "cherry", "familyA", "fiveValues", 500L, "cherry-fiveValues-4" },
            { "cherry", "familyA", "fiveValues", 400L, "cherry-fiveValues-3" },
            { "cherry", "familyA", "fiveValues", 300L, "cherry-fiveValues-2" },
            { "cherry", "familyA", "fiveValues", 200L, "cherry-fiveValues-1" },
            { "cherry", "familyA", "fiveValues", 100L, "cherry-fiveValues-0" },

            { "cherry", "familyA", "oneValueA",  100L, "cherry-oneValueA-0" },
        };
        runScanAssertions(query, expectedResults, 1);
    }

    @Test
    public void testVersionAndTimerangeColumnScan() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan 2 versions of familyB:fiveValues, " +
                "4 versions of familyC:tenValuesB between {ctbstart} and {ctbstop}, " +
                "familyA:tenValuesB between {atbstart} and {atbstop} " +
                "from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "cherry");
        // will result in 4 most recent versions of familyC:tenValuesB
        query.setTimestamp("ctbstart", 2000L);
        query.setTimestamp("ctbstop", 20000L);
        // will result in familyA:tenValuesB with timestamp 5000
        query.setTimestamp("atbstart", 3000L);
        query.setTimestamp("atbstop",  6000L);
        Object[][] expectedResults = new Object[][] {
            { "cherry", "familyA", "tenValuesB",  5000L, "cherry-tenValuesB-4" },

            { "cherry", "familyB", "fiveValues", 500L, "cherry-fiveValues-4" },
            { "cherry", "familyB", "fiveValues", 400L, "cherry-fiveValues-3" },

            { "cherry", "familyC", "tenValuesB", 10000L, "cherry-tenValuesB-9" },
            { "cherry", "familyC", "tenValuesB",  9000L, "cherry-tenValuesB-8" },
            { "cherry", "familyC", "tenValuesB",  8000L, "cherry-tenValuesB-7" },
            { "cherry", "familyC", "tenValuesB",  7000L, "cherry-tenValuesB-6" },
        };
        runScanAssertions(query, expectedResults, 1);
    }

    @Test
    public void testFamilyOnlyColumnScan() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan 2 versions of familyA:*, 4 versions of familyB:* from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "cherry");
        Object[][] expectedResults = new Object[][] {
            { "cherry", "familyA", "fiveValues",   500L, "cherry-fiveValues-4" },
            { "cherry", "familyA", "fiveValues",   400L, "cherry-fiveValues-3" },
            { "cherry", "familyA", "oneValueA",    100L, "cherry-oneValueA-0" },
            { "cherry", "familyA", "oneValueB",  10000L, "cherry-oneValueB-0" },
            { "cherry", "familyA", "tenValuesA",  1090L, "cherry-tenValuesA-9" },
            { "cherry", "familyA", "tenValuesA",  1080L, "cherry-tenValuesA-8" },
            { "cherry", "familyA", "tenValuesB", 10000L, "cherry-tenValuesB-9" },
            { "cherry", "familyA", "tenValuesB",  9000L, "cherry-tenValuesB-8" },

            { "cherry", "familyB", "fiveValues",   500L, "cherry-fiveValues-4" },
            { "cherry", "familyB", "fiveValues",   400L, "cherry-fiveValues-3" },
            { "cherry", "familyB", "fiveValues",   300L, "cherry-fiveValues-2" },
            { "cherry", "familyB", "fiveValues",   200L, "cherry-fiveValues-1" },
            { "cherry", "familyB", "oneValueA",    100L, "cherry-oneValueA-0" },
            { "cherry", "familyB", "oneValueB",  10000L, "cherry-oneValueB-0" },
            { "cherry", "familyB", "tenValuesA",  1090L, "cherry-tenValuesA-9" },
            { "cherry", "familyB", "tenValuesA",  1080L, "cherry-tenValuesA-8" },
            { "cherry", "familyB", "tenValuesA",  1070L, "cherry-tenValuesA-7" },
            { "cherry", "familyB", "tenValuesA",  1060L, "cherry-tenValuesA-6" },
            { "cherry", "familyB", "tenValuesB", 10000L, "cherry-tenValuesB-9" },
            { "cherry", "familyB", "tenValuesB",  9000L, "cherry-tenValuesB-8" },
            { "cherry", "familyB", "tenValuesB",  8000L, "cherry-tenValuesB-7" },
            { "cherry", "familyB", "tenValuesB",  7000L, "cherry-tenValuesB-6" },
        };
        runScanAssertions(query, expectedResults, 1);
    }

    @Test
    public void testQualiferPrefixColumnScan() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan familyA:oneValue*, familyA:fiveValues from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "cherry");
        Object[][] expectedResults = new Object[][] {
            { "cherry", "familyA", "fiveValues",   500L, "cherry-fiveValues-4" },
            { "cherry", "familyA", "oneValueA",    100L, "cherry-oneValueA-0" },
            { "cherry", "familyA", "oneValueB",  10000L, "cherry-oneValueB-0" },
        };
        runScanAssertions(query, expectedResults, 1);
    }

    /**
     * The Scan resets familyMap values when addFamily is called, so the column order
     * can have an effect on how the Scan is configured.  Test to make sure the builder
     * is immune to that.
     */
    @Test
    public void testQualiferPrefixColumnScanOppositeOrder() throws Exception {
        Query query = this.queryPlanner.parse(
                "scan familyA:fiveValues, familyA:oneValue* from " + TABLE_NAME + " where rowkey = {id}");
        query.setString("id", "cherry");
        Object[][] expectedResults = new Object[][] {
            { "cherry", "familyA", "fiveValues",   500L, "cherry-fiveValues-4" },
            { "cherry", "familyA", "oneValueA",    100L, "cherry-oneValueA-0" },
            { "cherry", "familyA", "oneValueB",  10000L, "cherry-oneValueB-0" },
        };
        runScanAssertions(query, expectedResults, 1);
    }

    private void runScanAssertions(Query query, Object[][] expectedResults, int expectedRowCount) throws Exception {
        assertThat("query is not null", query, is(notNullValue()));
        int rowCount = 0;
        int keyValueCount = 0;
        ResultScanner scanner = null;
        try {
            scanner = query.scan();
            assertThat("scanner is not null", scanner, is(notNullValue()));
            int resultIndex = 0;
            for (Result result : scanner) {
                KeyValue[] keyValues = result.raw();
                keyValueCount += keyValues.length;
                for (int i = 0; i < keyValues.length; i++) {
                    String expectedRowKey    = (String)expectedResults[resultIndex][0];
                    String expectedFamily    = (String)expectedResults[resultIndex][1];
                    String expectedQualifier = (String)expectedResults[resultIndex][2];
                    long expectedTimestamp   = (Long)expectedResults[resultIndex][3];
                    String expectedValue     = (String)expectedResults[resultIndex][4];

                    String expectedKV = expectedRowKey + "/" + expectedFamily + ":" +
                        expectedQualifier + "/" + expectedTimestamp;
                    String msg = " matches: " + expectedKV + " vs. " + keyValues[i];

                    assertThat("row key" + msg,
                            Bytes.toString(keyValues[i].getRow()),       is(expectedRowKey));
                    assertThat("family" + msg,
                            Bytes.toString(keyValues[i].getFamily()),    is(expectedFamily));
                    assertThat("qualifier" + msg,
                            Bytes.toString(keyValues[i].getQualifier()), is(expectedQualifier));
                    assertThat("timestamp" + msg,
                            keyValues[i].getTimestamp(),                 is(expectedTimestamp));
                    assertThat("value" + msg,
                            Bytes.toString(keyValues[i].getValue()),     is(expectedValue));
                    resultIndex++;
                }
                rowCount++;
            }
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
            if (query != null) {
                query.close();
            }
        }
        assertThat("row count is correct", rowCount, is(expectedRowCount));
        assertThat("key value count is correct", keyValueCount, is(expectedResults.length));
    }
}
