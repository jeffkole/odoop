package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

/**
 * Base class for tests related to AbstractColumnVersionTimerangeFilter
 *
 * @author jeff@opower.com
 */
public abstract class AbstractColumnVersionTimerangeFilterTestSupport {
    protected static final byte[] ROW_A = Bytes.toBytes("rowA");
    protected static final byte[] ROW_B = Bytes.toBytes("rowB");
    protected static final byte[] FAMILY_A = Bytes.toBytes("familyA");
    protected static final byte[] FAMILY_B = Bytes.toBytes("familyB");
    protected static final byte[] QUALIFIER_A = Bytes.toBytes("qualifierA");
    protected static final byte[] QUALIFIER_B = Bytes.toBytes("qualifierB");
    protected static final byte[] VALUE = Bytes.toBytes("value");
    protected static final KeyValue AAA = new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, 100L, VALUE);
    protected static final KeyValue AAB = new KeyValue(ROW_A, FAMILY_A, QUALIFIER_B, 100L, VALUE);
    protected static final KeyValue ABA = new KeyValue(ROW_A, FAMILY_B, QUALIFIER_A, 100L, VALUE);
    protected static final KeyValue ABB = new KeyValue(ROW_A, FAMILY_B, QUALIFIER_B, 100L, VALUE);
    protected static final KeyValue BAA = new KeyValue(ROW_B, FAMILY_A, QUALIFIER_A, 100L, VALUE);
    protected static final KeyValue BAB = new KeyValue(ROW_B, FAMILY_A, QUALIFIER_B, 100L, VALUE);
    protected static final KeyValue BBA = new KeyValue(ROW_B, FAMILY_B, QUALIFIER_A, 100L, VALUE);
    protected static final KeyValue BBB = new KeyValue(ROW_B, FAMILY_B, QUALIFIER_B, 100L, VALUE);

    protected void runFilterTest(String description, Filter filter, KeyValue[] keyValues, ReturnCode[] returnCodes) {
        filter.reset();
        assertEquals("mismatched fixture arrays", keyValues.length, returnCodes.length);
        for (int i = 0; i < keyValues.length; i++) {
            assertThat(description + ": " + keyValues[i].toString() + " should be " + returnCodes[i],
                    filter.filterKeyValue(keyValues[i]), is(returnCodes[i]));
        }
    }
}
