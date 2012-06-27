package com.opower.hadoop.hbase.selector;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;

/**
 * Inverses the contract of an HBase {@link Filter} so that it is more easily understood.  A {@link Selector}
 * selects rows or cells to be returned from a {@link Get} or {@link Scan}, whereas a {@link Filter} filters
 * them out.  To use a {@link Selector}, simply instantiate a {@link SelectorFilter} with the {@link Selector}
 * you want to use and set it on the {@link Get} or {@link Scan}.
 *
 * @author jeff@opower.com
 */
public interface Selector extends Writable {
    /**
     * Determine whether or not the row should be included in the results
     *
     * @param buffer a buffer containing the row key
     * @param offset offset into buffer where row key starts
     * @param length length of the row key
     * @return true to include the row if any cells match
     */
    boolean includeRow(byte[] buffer, int offset, int length);

    /**
     * Determine what to do with the key/value.  If {@link #includeRow} returned false, then this method will
     * not even be called.  Therefore, the only reasonable values to return are {@code INCLUDE, SKIP, NEXT_COL}.
     *
     * @param keyValue the KeyValue in question
     * @see Filter.ReturnCode
     * @see Filter#filterKeyValue(KeyValue)
     */
    ReturnCode handleKeyValue(KeyValue keyValue);
}
