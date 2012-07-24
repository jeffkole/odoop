package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.filter.FilterList;

import org.junit.Test;

/**
 * Tests the {@link FamilyOnlyColumnVersionTimerangeFilter} in a live HBase cluster
 *
 * @author jeff@opower.com
 */
public class IntTestFamilyOnlyColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilterIntTestSupport {
    @Test
    public void testMaxVersions() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "lauq-A", 6L, "value-A6" },
            { "row-A", "d", "lauq-A", 5L, "value-A5" },
            { "row-A", "d", "lauq-A", 4L, "value-A4" },
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
        runFilterAssertions(new FamilyOnlyColumnVersionTimerangeFilter(FAMILY, 3),
                expectedResults, 2);
    }

    @Test
    public void testTimeranges() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "lauq-A", 4L, "value-A4" },
            { "row-A", "d", "lauq-A", 3L, "value-A3" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-A", 3L, "value-A3" },
            { "row-A", "d", "qual-B", 4L, "value-B4" },
            { "row-A", "d", "qual-B", 3L, "value-B3" },
            { "row-B", "d", "qual-A", 4L, "value-A4" },
            { "row-B", "d", "qual-A", 3L, "value-A3" },
            { "row-B", "d", "qual-B", 4L, "value-B4" },
            { "row-B", "d", "qual-B", 3L, "value-B3" },
        };
        runFilterAssertions(new FamilyOnlyColumnVersionTimerangeFilter(FAMILY, 100, 3L, 5L),
                expectedResults, 2);
    }

    @Test
    public void testFilterPlaysNicelyInAFilterList() throws Exception {
        Object[][] expectedResults = new Object[][] {
            { "row-A", "d", "lauq-A", 6L, "value-A6" },
            { "row-A", "d", "lauq-A", 5L, "value-A5" },

            { "row-A", "d", "qual-A", 5L, "value-A5" },
            { "row-A", "d", "qual-A", 4L, "value-A4" },
            { "row-A", "d", "qual-B", 6L, "value-B6" },
            { "row-A", "d", "qual-B", 5L, "value-B5" },

            { "row-A", "e", "qual-A", 3L, "value-A3" },
            { "row-A", "e", "qual-A", 2L, "value-A2" },
            { "row-A", "e", "qual-A", 1L, "value-A1" },
            { "row-A", "e", "qual-B", 3L, "value-B3" },
            { "row-A", "e", "qual-B", 2L, "value-B2" },

            { "row-B", "d", "qual-A", 6L, "value-A6" },
            { "row-B", "d", "qual-A", 5L, "value-A5" },
            { "row-B", "d", "qual-B", 6L, "value-B6" },
            { "row-B", "d", "qual-B", 5L, "value-B5" },

            { "row-B", "e", "qual-A", 3L, "value-A3" },
            { "row-B", "e", "qual-A", 2L, "value-A2" },
            { "row-B", "e", "qual-B", 3L, "value-B3" },
        };
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new FamilyOnlyColumnVersionTimerangeFilter(FAMILY,   2, 3L, 7L));
        filterList.addFilter(new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_2, 3, 1L, 4L));
        runFilterAssertions(filterList, expectedResults, 2);
    }
}
