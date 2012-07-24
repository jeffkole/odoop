package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

/**
 * Tests the {@link ColumnVersionTimerangeFilter} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
public class IntTestColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilterIntTestSupport {
    @Test
    public void testMaxVersions() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "qual-A", 5L, "value-A5" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-A", 3L, "value-A3" },
            { "row-B", "d", "qual-A", 6L, "value-A6" },
            { "row-B", "d", "qual-A", 5L, "value-A5" },
            { "row-B", "d", "qual-A", 4L, "value-A4" },
        };
        runFilterAssertions(new ColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual-A"), 3),
                expectedResults, 2);
    }

    @Test
    public void testTimeranges() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "qual-B", 4L, "value-B4" },
            { "row-A", "d", "qual-B", 3L, "value-B3" },
            { "row-B", "d", "qual-B", 4L, "value-B4" },
            { "row-B", "d", "qual-B", 3L, "value-B3" },
        };
        runFilterAssertions(new ColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual-B"), 100, 3L, 5L),
                expectedResults, 2);
    }

    @Test
    public void testFilterPlaysNicelyInAFilterList() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "qual-A", 5L, "value-A5" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-B", 3L, "value-B3" },
            { "row-A", "d", "qual-B", 2L, "value-B2" },
            { "row-B", "d", "qual-A", 6L, "value-A6" },
            { "row-B", "d", "qual-A", 5L, "value-A5" },
            { "row-B", "d", "qual-B", 3L, "value-B3" },
        };
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new ColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual-A"), 2, 3L, 7L));
        filterList.addFilter(new ColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual-B"), 3, 1L, 4L));
        runFilterAssertions(filterList, expectedResults, 2);
    }
}
