package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Tests {@link AbstractKeyValueSelector}
 *
 * @author jeff@opower.com
 */
public class TestAbstractKeyValueSelector {
    private static class NoKeyValueSelector extends AbstractKeyValueSelector {
        @Override
        protected boolean includeKeyValue(KeyValue keyValue) {
            return false;
        }

        public void write(DataOutput out) throws IOException {
        }
        public void readFields(DataInput in) throws IOException {
        }
    }

    private static class AllKeyValueSelector extends AbstractKeyValueSelector {
        @Override
        protected boolean includeKeyValue(KeyValue keyValue) {
            return true;
        }

        public void write(DataOutput out) throws IOException {
        }
        public void readFields(DataInput in) throws IOException {
        }
    }

    @Test
    public void testNoKeyValueSelector() {
        Selector selector = new NoKeyValueSelector();
        assertTrue(selector.includeRow(null, 0, 0));
        assertEquals(ReturnCode.SKIP, selector.handleKeyValue(null));
    }

    @Test
    public void testAllKeyValueSelector() {
        Selector selector = new AllKeyValueSelector();
        assertTrue(selector.includeRow(null, 0, 0));
        assertEquals(ReturnCode.INCLUDE, selector.handleKeyValue(null));
    }
}
