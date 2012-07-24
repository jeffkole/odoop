package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

/**
 * Tests the {@link QualifierPrefixColumnVersionTimerangeFilter} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
public class IntTestQualifierPrefixColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilterIntTestSupport {
    @Test
    public void testMaxVersions() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "qual-A", 5L, "value-A5" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-A", 3L, "value-A3" },
            { "row-A", "d", "qual-B", 6L, "value-B6" },
            { "row-A", "d", "qual-B", 5L, "value-B5" },
            { "row-A", "d", "qual-B", 4L, "value-B4" },
            { "row-B", "d", "qual-A", 6L, "value-A6" },
            { "row-B", "d", "qual-A", 5L, "value-A5" },
            { "row-B", "d", "qual-A", 4L, "value-A4" },
            { "row-B", "d", "qual-B", 7L, "value-B7" },
            { "row-B", "d", "qual-B", 6L, "value-B6" },
            { "row-B", "d", "qual-B", 5L, "value-B5" },
        };
        runFilterAssertions(new QualifierPrefixColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual"), 3),
                expectedResults, 2);
    }

    @Test
    public void testTimeranges() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-A", 3L, "value-A3" },
            { "row-A", "d", "qual-B", 4L, "value-B4" },
            { "row-A", "d", "qual-B", 3L, "value-B3" },
            { "row-B", "d", "qual-A", 4L, "value-A4" },
            { "row-B", "d", "qual-A", 3L, "value-A3" },
            { "row-B", "d", "qual-B", 4L, "value-B4" },
            { "row-B", "d", "qual-B", 3L, "value-B3" },
        };
        runFilterAssertions(new QualifierPrefixColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual"), 100, 3L, 5L),
                expectedResults, 2);
    }

    @Test
    public void testFilterPlaysNicelyInAFilterList() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "lauq-A", 5L, "value-A5" },
            { "row-A", "d", "lauq-A", 4L, "value-A4" },
            { "row-A", "d", "lauq-A", 3L, "value-A3" },

            { "row-A", "d", "qual-A", 5L, "value-A5" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },

            { "row-A", "d", "qual-B", 6L, "value-B6" },
            { "row-A", "d", "qual-B", 5L, "value-B5" },

            { "row-B", "d", "qual-A", 6L, "value-A6" },
            { "row-B", "d", "qual-A", 5L, "value-A5" },

            { "row-B", "d", "qual-B", 6L, "value-B6" },
            { "row-B", "d", "qual-B", 5L, "value-B5" },
        };
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new QualifierPrefixColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("qual"), 2, 3L, 7L));
        filterList.addFilter(new QualifierPrefixColumnVersionTimerangeFilter(FAMILY, Bytes.toBytes("lauq"), 3, 1L, 6L));
        runFilterAssertions(filterList, expectedResults, 2);
    }
}
