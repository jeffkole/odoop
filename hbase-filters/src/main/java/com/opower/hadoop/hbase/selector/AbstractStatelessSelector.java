package com.opower.hadoop.hbase.selector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A base class to extend when the {@link Selector} you want to write has no state and thus does not
 * need specific implementations of the {@link org.apache.hadoop.io.Writable} methods.
 * The methods implemented here do absolutely nothing.
 *
 * @author jeff@opower.com
 */
public abstract class AbstractStatelessSelector implements Selector {
    /**
     * Does nothing at all
     */
    public final void write(DataOutput out) throws IOException {
    }

    /**
     * Does nothing at all
     */
    public final void readFields(DataInput in) throws IOException {
    }
}
