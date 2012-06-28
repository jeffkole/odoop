package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Tests the {@link SelectorFilter} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
public class IntTestSelectorFilter {
    private static HBaseTestingUtility hbaseTestingUtility;

    private byte[] tableName = Bytes.toBytes("filter_test");
    private byte[] family = new byte[] { 'd' };
    private HTable table;

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        hbaseTestingUtility = new HBaseTestingUtility();
        hbaseTestingUtility.startMiniCluster();
    }

    @Before
    public void setUp() throws Exception {
        this.table = hbaseTestingUtility.createTable(this.tableName, this.family, 30);
    }

    @After
    public void tearDown() throws Exception {
        this.hbaseTestingUtility.deleteTable(this.tableName);
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        hbaseTestingUtility.shutdownMiniCluster();
    }

    @Test
    public void testAllRowsWithSimpleCells() throws Exception {
        int rows = loadSimpleTable(this.table, this.family);
        runFilteredScanTest(new AllRowSelector(), this.table, this.family, rows);
    }

    @Test
    public void testAllRowsWithComplexCells() throws Exception {
        int rows = loadComplexTable(this.table, this.family);
        runFilteredScanTest(new AllRowSelector(), this.table, this.family, rows);
    }

    @Test
    public void testNoRowsWithSimpleCells() throws Exception {
        int rows = loadSimpleTable(this.table, this.family);
        runFilteredScanTest(new NoRowSelector(), this.table, this.family, 0);
    }

    @Test
    public void testNoRowsWithComplexCells() throws Exception {
        int rows = loadComplexTable(this.table, this.family);
        runFilteredScanTest(new NoRowSelector(), this.table, this.family, 0);
    }

    @Test
    public void testOddRowsWithSimpleCells() throws Exception {
        int rows = loadSimpleTable(this.table, this.family);
        runFilteredScanTest(new OddRowSelector(), this.table, this.family, rows / 2);
    }

    @Test
    public void testOddRowsWithComplexCells() throws Exception {
        int rows = loadComplexTable(this.table, this.family);
        runFilteredScanTest(new OddRowSelector(), this.table, this.family, rows / 2);
    }

    @Test
    public void testSpecificQualifierSelection() throws Exception {
        final byte[] qa = new byte[] { 'a' };
        final byte[] qb = new byte[] { 'b' };
        final byte[] qc = new byte[] { 'c' };
        int aVersions = 5;
        int bVersions = 1;
        int cVersions = 15;
        int rowsExpected = loadTable(this.table, this.family, qa, aVersions);
        loadTable(this.table, this.family, qb, bVersions);
        loadTable(this.table, this.family, qc, cVersions);

        runQualifierTest(qa, rowsExpected, aVersions);
        runQualifierTest(qb, rowsExpected, bVersions);
        runQualifierTest(qc, rowsExpected, cVersions);
    }

    private void runQualifierTest(byte[] qualifier, int rowsExpected, int versionsPerRow) throws Exception {
        Filter filter = new SelectorFilter(new QualifierSelector(qualifier));
        Scan scan = new Scan();
        scan.addFamily(this.family);
        scan.setFilter(filter);
        scan.setMaxVersions();
        int rowsReturned = 0;
        int keyValuesReturned = 0;
        for (Result result : this.table.getScanner(scan)) {
            rowsReturned++;
            KeyValue[] keyValues = result.raw();
            keyValuesReturned += keyValues.length;
            for (KeyValue keyValue : keyValues) {
                assertArrayEquals("Wrong qualifier", qualifier, keyValue.getQualifier());
            }
        }
        assertEquals("Wrong number of rows", rowsExpected, rowsReturned);
        assertEquals("Wrong number of key/values", versionsPerRow * rowsExpected, keyValuesReturned);
    }

    private void runFilteredScanTest(Selector selector, HTable table, byte[] family, int expectedRows) throws Exception {
        Filter filter = new SelectorFilter(selector);
        Scan scan = new Scan();
        scan.addFamily(family);
        scan.setFilter(filter);
        scan.setMaxVersions();
        int rows = 0;
        for (Result result : table.getScanner(scan)) {
            rows++;
        }
        assertEquals("Wrong number of rows", expectedRows, rows);
    }

    private int loadSimpleTable(HTable table, byte[] family) throws Exception {
        // loadTable loads rows from 'aaa' to 'zzz'
        return hbaseTestingUtility.loadTable(this.table, this.family);
    }

    private int loadComplexTable(HTable table, byte[] family) throws Exception {
        byte[] qa = new byte[] { 'a' };
        byte[] qb = new byte[] { 'b' };
        byte[] qc = new byte[] { 'c' };
        int rows = loadTable(table, family, qa, 5);
        loadTable(table, family, qb, 1);
        loadTable(table, family, qc, 15);
        return rows;
    }

    private int loadTable(HTable table, byte[] family, byte[] qualifier, int numVersions) throws Exception {
        table.setAutoFlush(false);
        int rowCount = 0;
        byte[] key = new byte[3];
        for (byte b1 = 'a'; b1 <= 'z'; b1++) {
            for (byte b2 = 'a'; b2 <= 'z'; b2++) {
                for (byte b3 = 'a'; b3 <= 'z'; b3++) {
                    rowCount++;
                    key[0] = b1;
                    key[1] = b2;
                    key[2] = b3;
                    Put put = new Put(key);
                    for (int i = 0; i < numVersions; i++) {
                        put.add(family, qualifier, (long)i, key);
                    }
                    table.put(put);
                }
            }
        }
        table.flushCommits();
        return rowCount;
    }

    /**
     * Selects all rows
     */
    public static class AllRowSelector extends AbstractStatelessSelector {
        @Override
        public boolean includeRow(byte[] buffer, int offset, int length) {
            return true;
        }
        @Override
        public ReturnCode handleKeyValue(KeyValue keyValue) {
            return ReturnCode.INCLUDE;
        }
    }

    /**
     * Selects no rows
     */
    public static class NoRowSelector extends AbstractStatelessSelector {
        @Override
        public boolean includeRow(byte[] buffer, int offset, int length) {
            return false;
        }
        @Override
        public ReturnCode handleKeyValue(KeyValue keyValue) {
            // this is irrelevant, since it should never be called
            return ReturnCode.INCLUDE;
        }
    }

    /**
     * Selects every other row, starting with the first, thus odd-numbered rows
     */
    public static class OddRowSelector extends AbstractStatelessSelector {
        private int rowCount = 0;
        @Override
        public boolean includeRow(byte[] buffer, int offset, int length) {
            if (++rowCount % 2 == 0) {
                return true;
            }
            return false;
        }
        @Override
        public ReturnCode handleKeyValue(KeyValue keyValue) {
            // this is irrelevant, since it should never be called
            return ReturnCode.INCLUDE;
        }
    }

    /**
     * Selects all rows, but only cells that match the specific qualifier
     */
    public static class QualifierSelector implements Selector {
        private byte[] qualifier;

        public QualifierSelector() {}

        public QualifierSelector(byte[] qualifier) {
            this.qualifier = qualifier;
        }

        @Override
        public boolean includeRow(byte[] buffer, int offset, int length) {
            return true;
        }
        @Override
        public ReturnCode handleKeyValue(KeyValue keyValue) {
            // compare the qualifier we want to the qualifier of the key/value.
            // (server-side key/value calls should use the buffers according to the documentation)
            if (Bytes.compareTo(this.qualifier, 0, this.qualifier.length,
                        keyValue.getBuffer(), keyValue.getQualifierOffset(), keyValue.getQualifierLength()) == 0) {
                return ReturnCode.INCLUDE;
            }
            return ReturnCode.NEXT_COL;
        }
        public void write(DataOutput out) throws IOException {
            Bytes.writeByteArray(out, this.qualifier);
        }
        public void readFields(DataInput in) throws IOException {
            this.qualifier = Bytes.readByteArray(in);
        }
    }
}
