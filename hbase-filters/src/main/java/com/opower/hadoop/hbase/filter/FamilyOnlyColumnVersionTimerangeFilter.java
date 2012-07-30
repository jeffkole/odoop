package com.opower.hadoop.hbase.filter;

import com.google.common.base.Objects;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Filters a column specified by family and based on a timerange and number of versions,
 * specifically meant to implement the column selection functionality of the Query Planner.
 *
 * @author jeff@opower.com
 */
public class FamilyOnlyColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilter {
    private byte[] family;

    private byte[] currentBuffer = new byte[0];
    private int currentQualifierOffset = 0;
    private int currentQualifierLength = 0;

    public FamilyOnlyColumnVersionTimerangeFilter() {
        super();
    }

    /**
     * Create a filter based on the family and maximum number of versions to
     * return.  The timerange will be effectiely ignored.
     *
     * @param family
     * @param maxVersions
     */
    public FamilyOnlyColumnVersionTimerangeFilter(byte[] family, int maxVersions) {
        this(family, maxVersions, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Create a filter based on the family, maximum number of versions, and a
     * timerange to check.  The timerange is inclusive at the beginning and exclusive on
     * the end: {@code [start, stop)}.
     *
     * @param family
     * @param maxVersions
     * @param start
     * @param stop
     */
    public FamilyOnlyColumnVersionTimerangeFilter(byte[] family, int maxVersions, long start, long stop) {
        super(maxVersions, start, stop);
        this.family = family;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("family", Bytes.toStringBinary(this.family))
            .add("maxVersions", getMaxVersions())
            .add("start", getStartTimestamp())
            .add("stop", getStopTimestamp())
            .toString();
    }

    public byte[] getFamily() {
        return this.family;
    }

    @Override
    protected boolean includeFamilyAndQualifier(KeyValue keyValue) {
        // include the key/value if it matches the family we care about
        return keyValue.matchingFamily(this.family);
    }

    @Override
    protected boolean resetVersionCount(KeyValue keyValue) {
        // reset the version count each time the qualifier changes
        boolean sameQualifier =
            keyValue.matchingQualifier(this.currentBuffer, this.currentQualifierOffset, this.currentQualifierLength);
        if (sameQualifier) {
            return false;
        }
        this.currentBuffer = keyValue.getBuffer();
        this.currentQualifierOffset = keyValue.getQualifierOffset();
        this.currentQualifierLength = keyValue.getQualifierLength();
        return true;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        Bytes.writeByteArray(out, this.family);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.family = Bytes.readByteArray(in);
    }
}
