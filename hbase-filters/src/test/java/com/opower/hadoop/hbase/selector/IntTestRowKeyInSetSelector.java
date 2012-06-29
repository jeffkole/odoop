package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import com.opower.hadoop.hbase.test.HBaseTestRunner;

import static org.junit.Assert.*;

/**
 * Tests the {@link RowKeyInSetSelector} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
@RunWith(HBaseTestRunner.class)
public class IntTestRowKeyInSetSelector {
    private HBaseTestingUtility hbaseTestingUtility;

    private byte[] tableName = Bytes.toBytes("row_key_in_set_filter");
    private byte[] family = new byte[] { 'd' };
    private HTable table;

    @Before
    public void setUp() throws Exception {
        this.table = hbaseTestingUtility.createTable(this.tableName, this.family, 30);
    }

    @After
    public void tearDown() throws Exception {
        this.hbaseTestingUtility.deleteTable(this.tableName);
    }

    @Test
    public void testRowsInSetWithSimpleCells() throws Exception {
        // loadTable loads rows from 'aaa' to 'zzz'
        hbaseTestingUtility.loadTable(this.table, this.family);
        runFilteredScanTest(this.table, this.family);
    }

    @Test
    public void testRowsInSetWithComplexCells() throws Exception {
        byte[] qa = new byte[] { 'a' };
        byte[] qb = new byte[] { 'b' };
        byte[] qc = new byte[] { 'c' };
        loadTable(this.table, this.family, qa, 5);
        loadTable(this.table, this.family, qb, 1);
        loadTable(this.table, this.family, qc, 15);
        runFilteredScanTest(this.table, this.family);
    }

    private void runFilteredScanTest(HTable table, byte[] family) throws Exception {
        Map<String, Boolean> keys = new HashMap<String, Boolean>();
        keys.put("abc", false);
        keys.put("xyz", false);
        keys.put("mno", false);
        keys.put("jbk", false);
        keys.put("non-existent key", false);

        Filter filter = new SelectorFilter(new RowKeyInSetSelector(keys.keySet()));
        Scan scan = new Scan();
        scan.addFamily(family);
        scan.setFilter(filter);
        scan.setMaxVersions();
        for (Result result : table.getScanner(scan)) {
            String key = Bytes.toString(result.getRow());
            assertTrue("unexpected key found: " + key, keys.containsKey(key));
            keys.put(key, true);
        }
        for (String key : keys.keySet()) {
            if ("non-existent key".equals(key)) {
                assertFalse("found key that should not exist", keys.get(key));
            }
            else {
                assertTrue("missed key " + key, keys.get(key));
            }
        }
    }

    private void loadTable(HTable table, byte[] family, byte[] qualifier, int numVersions) throws Exception {
        table.setAutoFlush(false);
        byte[] key = new byte[3];
        for (byte b1 = 'a'; b1 <= 'z'; b1++) {
            for (byte b2 = 'a'; b2 <= 'z'; b2++) {
                for (byte b3 = 'a'; b3 <= 'z'; b3++) {
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
    }
}
