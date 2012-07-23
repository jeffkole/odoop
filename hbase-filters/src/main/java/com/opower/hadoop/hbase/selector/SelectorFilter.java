package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.opower.common.reflect.Reflection;

/**
 * Adapts a {@link Selector} to fit the contract of a {@link org.apache.hadoop.hbase.filter.Filter}.
 * When you want to use a {@link Selector} simply instantiate a {@link SelectorFilter} to wrap it
 * up and set it on a {@code Scan} or {@code Get}.
 *
 * @author jeff@opower.com
 */
public class SelectorFilter extends FilterBase {
    private Selector selector;

    // transient field to track progress through the filter lifecycle
    private boolean includeRow = false;

    public SelectorFilter() {}

    /**
     * Construct a {@link org.apache.hadoop.hbase.filter.Filter} that wraps a {@link Selector}.
     * The selector passed in must be of a publicly accessible class, otherwise deserialization
     * on the server will not work.
     *
     * @param selector an instance of a publicly accessible Selector
     */
    public SelectorFilter(Selector selector) {
        if (selector == null) {
            throw new IllegalArgumentException("Selector must not be null");
        }
        Reflection.checkDeserializable(selector);
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
        // Do not use WritableFactories.newInstance, because it uses ReflectionUtils which caches
        // constructors, which in turn means these classes will never be garbage collected, which
        // is pretty bad if you want them to be used in a deployed filter context.
        String className = Bytes.toString(Bytes.readByteArray(in));
        try {
            Class selectorClass = Class.forName(className);
            this.selector = (Selector)selectorClass.newInstance();
            this.selector.readFields(in);
        }
        catch (Exception e) {
            throw new IOException("Error instantiating class " + className, e);
        }
    }
}
