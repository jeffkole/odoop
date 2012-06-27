package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Adapts a {@link Selector} to fit the contract of a {@link Filter}.  When you want to use a {@link Selector} simply
 * instantiate a {@link SelectorFilter} to wrap it up and set it on a {@link Scan} or {@link Get}.
 *
 * @author jeff@opower.com
 */
public class SelectorFilter extends FilterBase {
    private Selector selector;

    // transient field to track progress through the filter lifecycle
    private boolean includeRow = false;

    public SelectorFilter() {}

    public SelectorFilter(Selector selector) {
        this.selector = selector;
    }

    @Override
    public void reset() {
        this.includeRow = false;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (!this.includeRow) {
            return ReturnCode.NEXT_ROW;
        }
        return this.selector.handleKeyValue(keyValue);
    }

    @Override
    public boolean filterRow() {
        return !this.includeRow;
    }

    @Override
    public boolean filterRowKey(byte[] rowKeyBuffer, int offset, int length) {
        this.includeRow = this.selector.includeRow(rowKeyBuffer, offset, length);
        return !this.includeRow;
    }

    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, Bytes.toBytes(this.selector.getClass().getName()));
        this.selector.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.selector = (Selector)createForName(Bytes.toString(Bytes.readByteArray(in)));
        this.selector.readFields(in);
    }

    private Writable createForName(String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Writable> clazz = (Class<? extends Writable>)Class.forName(className);
            return WritableFactories.newInstance(clazz, new Configuration());
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to find class " + className);
        }
    }
}
