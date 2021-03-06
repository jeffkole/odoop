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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.opower.hadoop.hbase.test.HBaseTestRunner;

import static org.junit.Assert.*;

/**
 * Tests the {@link SelectorFilter} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
@RunWith(HBaseTestRunner.class)
public class IntTestSelectorFilter {
    private static HBaseTestingUtility hbaseTestingUtility;

    private static byte[] tableNameSimple = Bytes.toBytes("filter_test_simple");
    private static byte[] tableNameComplex = Bytes.toBytes("filter_test_complex");
    private static byte[] family = new byte[] { 'd' };
    private static HTable tableSimple;
    private static HTable tableComplex;
    private static int rowsSimple;
    private static int rowsComplex;

    @BeforeClass
    public static void setUpClass() throws Exception {
        tableSimple = hbaseTestingUtility.createTable(tableNameSimple, family, 30);
        tableComplex = hbaseTestingUtility.createTable(tableNameComplex, family, 30);
        rowsSimple = loadSimpleTable(tableSimple, family);
        rowsComplex = loadComplexTable(tableComplex, family);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseTestingUtility.deleteTable(tableNameSimple);
        hbaseTestingUtility.deleteTable(tableNameComplex);
    }

    @Test
    public void testAllRowsWithSimpleCells() throws Exception {
        runFilteredScanTest(new AllRowSelector(), tableSimple, family, rowsSimple);
    }

    @Test
    public void testAllRowsWithComplexCells() throws Exception {
        runFilteredScanTest(new AllRowSelector(), tableComplex, family, rowsComplex);
    }

    @Test
    public void testNoRowsWithSimpleCells() throws Exception {
        runFilteredScanTest(new NoRowSelector(), tableSimple, family, 0);
    }

    @Test
    public void testNoRowsWithComplexCells() throws Exception {
        runFilteredScanTest(new NoRowSelector(), tableComplex, family, 0);
    }

    @Test
    public void testOddRowsWithSimpleCells() throws Exception {
        runFilteredScanTest(new OddRowSelector(), tableSimple, family, rowsSimple / 2);
    }

    @Test
    public void testOddRowsWithComplexCells() throws Exception {
        runFilteredScanTest(new OddRowSelector(), tableComplex, family, rowsComplex / 2);
    }

    @Test
    public void testSpecificQualifierSelection() throws Exception {
        // these must match the qualifiers and versions used in loadComplexTable
        final byte[] qa = new byte[] { 'a' };
        final byte[] qb = new byte[] { 'b' };
        final byte[] qc = new byte[] { 'c' };
        int aVersions = 5;
        int bVersions = 1;
        int cVersions = 15;

        runQualifierTest(tableComplex, qa, rowsComplex, aVersions);
        runQualifierTest(tableComplex, qb, rowsComplex, bVersions);
        runQualifierTest(tableComplex, qc, rowsComplex, cVersions);
    }

    private void runQualifierTest(HTable table, byte[] qualifier, int rowsExpected, int versionsPerRow) throws Exception {
        Filter filter = new SelectorFilter(new QualifierSelector(qualifier));
        Scan scan = new Scan();
        scan.addFamily(family);
        scan.setFilter(filter);
        scan.setMaxVersions();
        int rowsReturned = 0;
        int keyValuesReturned = 0;
        for (Result result : table.getScanner(scan)) {
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

    private static int loadSimpleTable(HTable table, byte[] family) throws Exception {
        // loadTable loads rows from 'aaa' to 'zzz'
        return hbaseTestingUtility.loadTable(table, family);
    }

    private static int loadComplexTable(HTable table, byte[] family) throws Exception {
        byte[] qa = new byte[] { 'a' };
        byte[] qb = new byte[] { 'b' };
        byte[] qc = new byte[] { 'c' };
        int rows = loadTable(table, family, qa, 5);
        loadTable(table, family, qb, 1);
        loadTable(table, family, qc, 15);
        return rows;
    }

    private static int loadTable(HTable table, byte[] family, byte[] qualifier, int numVersions) throws Exception {
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
