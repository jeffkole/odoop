package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;

/**
 * An abstract base class for {@link Selector Selectors} that focus on the key/value only.
 * By default all rows will be included.
 *
 * @author jeff@opower.com
 */
public abstract class AbstractKeyValueSelector implements Selector {
    /**
     * Always includes the row
     *
     * {@inheritDoc}
     */
    @Override
    public boolean includeRow(byte[] buffer, int offset, int length) {
        return true;
    }

    /**
     * Will include the key/value if {@link #includeKeyValue} returns true; otheriwse
     * the key/value is skipped.  Note that there is no way to give a hint to the scanner,
     * skip to the next column, or skip to the next row.
     *
     * {@inheritDoc}
     */
    @Override
    public final ReturnCode handleKeyValue(KeyValue keyValue) {
        if (includeKeyValue(keyValue)) {
            return ReturnCode.INCLUDE;
        }
        return ReturnCode.SKIP;
    }

    /**
     * Determine whether or not to include the {@link KeyValue} in the result set
     *
     * @param keyValue
     * @return true to include it, false otherwise
     */
    protected abstract boolean includeKeyValue(KeyValue keyValue);
}
