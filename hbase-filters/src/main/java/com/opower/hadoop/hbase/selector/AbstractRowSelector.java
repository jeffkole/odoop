package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;

/**
 * An abstract base class for making {@link Selector Selectors} that are based only on the row key
 *
 * @author jeff@opower.com
 */
public abstract class AbstractRowSelector implements Selector {
    /**
     * {@inheritDoc}
     * </p><p>
     * Always indicates that the key/value should be included, because the real logic
     * will be implemented in {@link Selector#includeRow(byte[], int, int)} to determine
     * if the row should be included or not.
     */
    @Override
    public final ReturnCode handleKeyValue(KeyValue keyValue) {
        return ReturnCode.INCLUDE;
    }
}
