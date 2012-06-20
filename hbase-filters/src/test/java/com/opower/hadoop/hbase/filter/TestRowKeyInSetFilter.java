package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Tests the {@link RowKeyInSetFilter} as if it were called by HBase
 *
 * @author jeff@opower.com
 */
public class TestRowKeyInSetFilter {
    private Filter mainFilter;
    private Set<String> inSet;
    private int setSize = 100;
    // We need to use a very small error rate for our tests, but we cannot use
    // 0, because that results in a runtime error.
    private float errorRate = 0.0001f;
    private int foldFactor = 0;

    @Before
    public void setUp() throws Exception {
        this.inSet = new HashSet<String>(this.setSize);
        ByteBloomFilter bloomFilter = new ByteBloomFilter(this.setSize,
                this.errorRate, Hash.JENKINS_HASH, this.foldFactor);
        bloomFilter.allocBloom();
        for (int i = 0; i < this.setSize; i++) {
            String key = UUID.randomUUID().toString();
            this.inSet.add(key);
            bloomFilter.add(Bytes.toBytesBinary(key));
        }
        // mimic the operations that the filter goes through in the real
        // lifecycle, which starts with a serialization before being
        // sent to the server for the real work to occur
        Filter filter = new RowKeyInSetFilter(bloomFilter);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);
        filter.write(out);
        out.flush();
        out.close();
        byte[] filterBytes = byteStream.toByteArray();

        // Reconstruct it before running it through the tests
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(filterBytes));
        this.mainFilter = new RowKeyInSetFilter();
        this.mainFilter.readFields(in);
    }

    @Test
    public void testRowsInSet() {
        runRowsInSetTest(this.mainFilter);
    }

    @Test
    public void testRowNotInSet() {
        runRowsNotInSetTest(this.mainFilter);
    }

    @Test
    public void testMixOfRowsInAndNotInSet() {
        runRowsNotInSetTest(this.mainFilter);
        runRowsInSetTest(this.mainFilter);
        runRowsNotInSetTest(this.mainFilter);
    }

    private void runRowsInSetTest(Filter filter) {
        for (String rowKey : this.inSet) {
            runFilterLifecycle(filter, Bytes.toBytesBinary(rowKey), true);
        }
    }

    private void runRowsNotInSetTest(Filter filter) {
        runFilterLifecycle(filter, Bytes.toBytesBinary(
                    "this does not even look like a UUID"), false);
    }

    private void runFilterLifecycle(Filter filter, byte[] rowKey, boolean inSet) {
        KeyValue keyValue = new KeyValue(rowKey, null, null);
        List<KeyValue> keyValues = Arrays.<KeyValue>asList(keyValue);

        filter.reset();
        assertFalse(filter.filterAllRemaining());
        if (inSet) {
            assertFalse(filter.filterRowKey(rowKey, 0, rowKey.length));
            assertEquals(Filter.ReturnCode.INCLUDE, filter.filterKeyValue(keyValue));
            filter.filterRow(keyValues);
            assertFalse(filter.filterRow());
        }
        else {
            assertTrue(filter.filterRowKey(rowKey, 0, rowKey.length));
            assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterKeyValue(keyValue));
            filter.filterRow(keyValues);
            assertTrue(filter.filterRow());
        }
        assertFalse(filter.filterAllRemaining());
    }
}
