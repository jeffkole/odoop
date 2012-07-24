package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests FamilyOnlyColumnVersionTimerangeFilter
 *
 * @author jeff@opower.com
 */
public class TestFamilyOnlyColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilterTestSupport {
    @Test
    public void testAlwaysSkipsColumnThatIsNotCaredAbout() {
        Filter filter = new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_A, 1);
        runFilterTest("a", filter, new KeyValue[] {
            ABA,
            ABB,
            ABB,
        }, new ReturnCode[] {
            ReturnCode.SKIP,
            ReturnCode.SKIP,
            ReturnCode.SKIP,
        });
    }

    @Test
    public void testConfiguredColumnIsIncluded() {
        Filter filter = new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_A, 1);
        runFilterTest("a", filter, new KeyValue[] {
            AAA,
        }, new ReturnCode[] {
            ReturnCode.INCLUDE
        });
    }

    @Test
    public void testColumnIsSkippedAfterEnoughVersionsHaveBeenFound() {
        int numVersions = 3;
        Filter filter = new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_A, numVersions);
        runFilterTest("a 3 versions", filter, new KeyValue[] {
            AAA,
            AAB,
            AAB,
            AAB,
            AAB,
            ABB,
        }, new ReturnCode[] {
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.SKIP,
            ReturnCode.SKIP,
        });
    }

    @Test
    public void testRowResetsColumnVersionCount() {
        int numVersions = 3;
        Filter filter = new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_A, numVersions);
        runFilterTest("a 3 versions, row A", filter, new KeyValue[] {
            AAA,
            AAB,
            AAB,
            AAB,
            AAB,
            ABB,
        }, new ReturnCode[] {
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.SKIP,
            ReturnCode.SKIP,
        });
        runFilterTest("a 3 versions, row B", filter, new KeyValue[] {
            BAA,
            BAB,
            BAB,
            BAB,
            BAB,
            BBB,
        }, new ReturnCode[] {
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
            ReturnCode.SKIP,
            ReturnCode.SKIP,
        });
    }

    @Test
    public void testTimerangeRestrictsMatchingKeyValues() {
        Filter filter = new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_A, 1000, 100L, 200L);
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        List<ReturnCode> returnCodes = new ArrayList<ReturnCode>();
        for (long ts = 90L; ts < 100L; ts++) {
            keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
            returnCodes.add(ReturnCode.SKIP);
        }
        for (long ts = 100L; ts < 200L; ts++) {
            keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
            returnCodes.add(ReturnCode.INCLUDE);
        }
        for (long ts = 200L; ts < 210L; ts++) {
            keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
            returnCodes.add(ReturnCode.SKIP);
        }
        runFilterTest("a timerange restriction", filter,
                keyValues.toArray(new KeyValue[0]),
                returnCodes.toArray(new ReturnCode[0]));
    }

    @Test
    public void testTimerangeRestrictsMatchingUpToMaxVersions() {
        int numVersions = 10;
        Filter filter = new FamilyOnlyColumnVersionTimerangeFilter(FAMILY_A, numVersions, 100L, 200L);
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        List<ReturnCode> returnCodes = new ArrayList<ReturnCode>();
        for (long ts = 90L; ts < 100L; ts++) {
            keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
            returnCodes.add(ReturnCode.SKIP);
        }
        int i = 0;
        for (long ts = 100L; ts < 200L; ts++) {
            if (i < numVersions) {
                keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
                returnCodes.add(ReturnCode.INCLUDE);
            }
            else {
                keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
                returnCodes.add(ReturnCode.SKIP);
            }
            i++;
        }
        for (long ts = 200L; ts < 210L; ts++) {
            keyValues.add(new KeyValue(ROW_A, FAMILY_A, QUALIFIER_A, ts, VALUE));
            returnCodes.add(ReturnCode.SKIP);
        }
        runFilterTest("a timerange version restrictions", filter,
                keyValues.toArray(new KeyValue[0]),
                returnCodes.toArray(new ReturnCode[0]));
    }
}
