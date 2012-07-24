package com.opower.hadoop.hbase.filter;

import com.google.common.base.Objects;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Filters a column based on a timerange and number of versions, specifically meant to
 * implement the column selection functionality of the Query Planner.
 *
 * @author jeff@opower.com
 */
public class QualifierPrefixColumnVersionTimerangeFilter extends FamilyOnlyColumnVersionTimerangeFilter {
    private byte[] qualifierPrefix;

    public QualifierPrefixColumnVersionTimerangeFilter() {
        super();
    }

    /**
     * Create a filter based on the family, a qualifier prefix, and the maximum
     * number of versions to return.  The timerange will be effectiely ignored.
     *
     * @param family
     * @param qualifierPrefix
     * @param maxVersions
     */
    public QualifierPrefixColumnVersionTimerangeFilter(byte[] family, byte[] qualifierPrefix, int maxVersions) {
        this(family, qualifierPrefix, maxVersions, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Create a filter based on the family, a qualifier prefix, the maximum number
     * of versions, and a timerange to check.  The timerange is inclusive at the
     * beginning and exclusive on the end: {@code [start, stop)}.
     *
     * @param family
     * @param qualifierPrefix
     * @param maxVersions
     * @param start
     * @param stop
     */
    public QualifierPrefixColumnVersionTimerangeFilter(byte[] family, byte[] qualifierPrefix, int maxVersions,
            long start, long stop) {
        super(family, maxVersions, start, stop);
        this.qualifierPrefix = qualifierPrefix;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("family", getFamily())
            .add("qualifierPrefix", Bytes.toStringBinary(this.qualifierPrefix))
            .add("maxVersions", getMaxVersions())
            .add("start", getStartTimestamp())
            .add("stop", getStopTimestamp())
            .toString();
    }

    public byte[] getQualifierPrefix() {
        return this.qualifierPrefix;
    }

    @Override
    protected boolean includeFamilyAndQualifier(KeyValue keyValue) {
        // include the key/value if it matches the family we care about
        // and the qualifier is prefixed by our qualifier prefix
        boolean familyMatches = keyValue.matchingFamily(getFamily());
        if (!familyMatches) {
            return false;
        }
        boolean qualifierLongEnough = keyValue.getQualifierLength() >= this.qualifierPrefix.length;
        if (!qualifierLongEnough) {
            return false;
        }
        // if our qualifier prefix equals the prefix of the qualifier, then include it
        int q = Bytes.compareTo(this.qualifierPrefix, 0, this.qualifierPrefix.length,
                keyValue.getBuffer(), keyValue.getQualifierOffset(), this.qualifierPrefix.length);
        return (q == 0);
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        Bytes.writeByteArray(out, this.qualifierPrefix);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.qualifierPrefix = Bytes.readByteArray(in);
    }
}
