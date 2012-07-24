package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests QualifierPrefixColumnVersionTimerangeFilter
 *
 * @author jeff@opower.com
 */
public class TestQualifierPrefixColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilterTestSupport {
    private static final byte[] QUALIFIER_PREFIX = Bytes.toBytes("qualifier");
    private static final byte[] BAD_QUALIFIER_PREFIX = Bytes.toBytes("junk");

    @Test
    public void testAlwaysSkipsColumnThatIsNotCaredAbout() {
        Filter filter = new QualifierPrefixColumnVersionTimerangeFilter(FAMILY_A, BAD_QUALIFIER_PREFIX, 1);
        runFilterTest("a:junk", filter, new KeyValue[] {
            AAB,
            ABA,
            ABB,
        }, new ReturnCode[] {
            ReturnCode.SKIP,
            ReturnCode.SKIP,
            ReturnCode.SKIP,
        });
    }

    @Test
    public void testConfiguredColumnIsIncluded() {
        Filter filter = new QualifierPrefixColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_PREFIX, 1);
        runFilterTest("a:qualifier", filter, new KeyValue[] {
            AAA,
            AAB,
        }, new ReturnCode[] {
            ReturnCode.INCLUDE,
            ReturnCode.INCLUDE,
        });
    }

    @Test
    public void testColumnIsSkippedAfterEnoughVersionsHaveBeenFound() {
        int numVersions = 3;
        Filter filter = new QualifierPrefixColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_PREFIX, numVersions);
        runFilterTest("a:qualifier 3 versions", filter, new KeyValue[] {
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
        Filter filter = new QualifierPrefixColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_PREFIX, numVersions);
        runFilterTest("a:qualifier 3 versions, row A", filter, new KeyValue[] {
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
        runFilterTest("a:qualifier 3 versions, row B", filter, new KeyValue[] {
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
        Filter filter = new QualifierPrefixColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_PREFIX, 1000, 100L, 200L);
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
        runFilterTest("a:qualifier timerange restriction", filter,
                keyValues.toArray(new KeyValue[0]),
                returnCodes.toArray(new ReturnCode[0]));
    }

    @Test
    public void testTimerangeRestrictsMatchingUpToMaxVersions() {
        int numVersions = 10;
        Filter filter = new QualifierPrefixColumnVersionTimerangeFilter(FAMILY_A, QUALIFIER_PREFIX, numVersions, 100L, 200L);
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
        runFilterTest("a:qualifier timerange version restrictions", filter,
                keyValues.toArray(new KeyValue[0]),
                returnCodes.toArray(new ReturnCode[0]));
    }
}
