package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

/**
 * Tests ColumnVersionTimerangeFilter
 *
 * @author jeff@opower.com
 */
public class TestColumnVersionTimerangeFilter {
    private static final byte[] ROW_A = Bytes.toBytes("rowA");
    private static final byte[] ROW_B = Bytes.toBytes("rowB");
    private static final byte[] FAMILY_A = Bytes.toBytes("familyA");
    private static final byte[] FAMILY_B = Bytes.toBytes("familyB");
    private static final byte[] QUALIFIER_A = Bytes.toBytes("qualifierA");
    private static final byte[] QUALIFIER_B = Bytes.toBytes("qualifierB");
    private static final byte[] VALUE = Bytes.toBytes("value");

    @Test
    public void testAlwaysSkipsColumnThatIsNotCaredAbout() {
        Filter filter = new ColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_A, 1);
        filter.reset();
        assertThat("a:b is skipped", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_B, 100L, VALUE)),
                is(ReturnCode.SKIP));
        assertThat("b:a is skipped", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_B, QUALIFIER_A, 100L, VALUE)),
                is(ReturnCode.SKIP));
        assertThat("b:b is skipped", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_B, QUALIFIER_B, 100L, VALUE)),
                is(ReturnCode.SKIP));
    }

    @Test
    public void testConfiguredColumnIsIncluded() {
        Filter filter = new ColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_A, 1);
        filter.reset();
        assertThat("a:a is included", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, 100L, VALUE)),
                is(ReturnCode.INCLUDE));
    }

    @Test
    public void testColumnIsSkippedAfterEnoughVersionsHaveBeenFound() {
        int numVersions = 3;
        Filter filter = new ColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_B, numVersions);
        filter.reset();
        assertThat("a:a is skipped", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, 100L, VALUE)),
                is(ReturnCode.SKIP));
        for (int i = 0; i < numVersions; i++) {
            assertThat(i + " a:b is included", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_B, 100L, VALUE)),
                    is(ReturnCode.INCLUDE));
        }
        assertThat("a:b is skipped", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_B, 100L, VALUE)),
                is(ReturnCode.SKIP));
        assertThat("b:b is skipped", filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_B, QUALIFIER_B, 100L, VALUE)),
                is(ReturnCode.SKIP));
    }

    @Test
    public void testRowResetsColumnVersionCount() {
        int numVersions = 5;
        Filter filter = new ColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_B, numVersions);

        byte[][] rows = new byte[][] { ROW_A, ROW_B };
        for (byte[] row : rows) {
            filter.reset();
            assertThat("a:a is skipped", filter.filterKeyValue(new KeyValue(row, FAMILY_A, QUALIFIER_A, 100L, VALUE)),
                    is(ReturnCode.SKIP));
            for (int i = 0; i < numVersions; i++) {
                assertThat(i + " a:b is included",
                        filter.filterKeyValue(new KeyValue(row, FAMILY_A, QUALIFIER_B, 100L, VALUE)),
                        is(ReturnCode.INCLUDE));
            }
            assertThat("a:b is skipped", filter.filterKeyValue(new KeyValue(row, FAMILY_A, QUALIFIER_B, 100L, VALUE)),
                    is(ReturnCode.SKIP));
            assertThat("b:b is skipped", filter.filterKeyValue(new KeyValue(row, FAMILY_B, QUALIFIER_B, 100L, VALUE)),
                    is(ReturnCode.SKIP));
        }
    }

    @Test
    public void testTimerangeRestrictsMatchingKeyValues() {
        Filter filter = new ColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_A, 1000, 100L, 200L);
        for (long ts = 90L; ts < 100L; ts++) {
            assertThat("a:a out of range is skipped",
                    filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                    is(ReturnCode.SKIP));
        }
        for (long ts = 100L; ts < 200L; ts++) {
            assertThat("a:a in range is included",
                    filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                    is(ReturnCode.INCLUDE));
        }
        for (long ts = 200L; ts < 210L; ts++) {
            assertThat("a:a out of range is skipped",
                    filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                    is(ReturnCode.SKIP));
        }
    }

    @Test
    public void testTimerangeRestrictsMatchingUpToMaxVersions() {
        int numVersions = 10;
        Filter filter = new ColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_A, numVersions, 100L, 200L);
        for (long ts = 90L; ts < 100L; ts++) {
            assertThat("a:a out of range is skipped",
                    filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                    is(ReturnCode.SKIP));
        }
        int i = 0;
        for (long ts = 100L; ts < 200L; ts++) {
            if (i < numVersions) {
                assertThat("a:a in range is included",
                        filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                        is(ReturnCode.INCLUDE));
            }
            else {
                assertThat("a:a in range is skipped after hitting max",
                        filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                        is(ReturnCode.SKIP));
            }
            i++;
        }
        for (long ts = 200L; ts < 210L; ts++) {
            assertThat("a:a out of range is skipped",
                    filter.filterKeyValue(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE)),
                    is(ReturnCode.SKIP));
        }
    }
}
