package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Tests the {@link RowKeyInSetSelector}
 *
 * @author jeff@opower.com
 */
public class TestRowKeyInSetSelector {
    private Set<String> inSet;
    private int setSize = 100;
    // We need to use a very small error rate for our tests, but we cannot use 0, because that results in a runtime error.
    private float errorRate = 0.0001f;
    private int foldFactor = 10;

    @Before
    public void setUp() throws Exception {
        this.inSet = new HashSet<String>(this.setSize);
        for (int i = 0; i < this.setSize; i++) {
            String key = UUID.randomUUID().toString();
            this.inSet.add(key);
        }
    }

    @Test
    public void testBloomFilterConstructor() {
        ByteBloomFilter bloomFilter = new ByteBloomFilter(this.setSize, this.errorRate, Hash.JENKINS_HASH, this.foldFactor);
        bloomFilter.allocBloom();
        for (String key : this.inSet) {
            bloomFilter.add(Bytes.toBytesBinary(key));
        }
        Selector selector = new RowKeyInSetSelector(bloomFilter);
        runRowsInSetTest(selector);
        runRowsNotInSetTest(selector);
        runRowsInSetTest(selector);
    }

    @Test
    public void testSetConstructor() {
        Selector selector = new RowKeyInSetSelector(this.inSet);
        runRowsInSetTest(selector);
        runRowsNotInSetTest(selector);
        runRowsInSetTest(selector);
    }

    private void runRowsInSetTest(Selector selector) {
        for (String rowKey : this.inSet) {
            byte[] rowKeyBytes = Bytes.toBytesBinary(rowKey);
            assertTrue(selector.includeRow(rowKeyBytes, 0, rowKeyBytes.length));
        }
    }

    private void runRowsNotInSetTest(Selector selector) {
        byte[] rowKeyBytes = Bytes.toBytesBinary("this does not even look like a UUID");
        assertFalse(selector.includeRow(rowKeyBytes, 0, rowKeyBytes.length));
    }
}
