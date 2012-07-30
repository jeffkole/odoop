package com.opower.hadoop.hbase.filter;

import com.google.common.base.Objects;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Filters a column selected by family and qualifier and based on a timerange and number
 * of versions, specifically meant to implement the column selection functionality of
 * the Query Planner.
 *
 * @author jeff@opower.com
 */
public class ColumnVersionTimerangeFilter extends AbstractColumnVersionTimerangeFilter {
    private byte[] family;
    private byte[] qualifier;

    public ColumnVersionTimerangeFilter() {
        super();
    }

    /**
     * Create a filter based on the family, qualifier, and maximum number of versions to
     * return.  The timerange will be effectiely ignored.
     *
     * @param family
     * @param qualifier
     * @param maxVersions
     */
    public ColumnVersionTimerangeFilter(byte[] family, byte[] qualifier, int maxVersions) {
        this(family, qualifier, maxVersions, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Create a filter based on the family, qualifier, maximum number of versions, and a
     * timerange to check.  The timerange is inclusive at the beginning and exclusive on
     * the end: {@code [start, stop)}.
     *
     * @param family
     * @param qualifier
     * @param maxVersions
     * @param start
     * @param stop
     */
    public ColumnVersionTimerangeFilter(byte[] family, byte[] qualifier, int maxVersions, long start, long stop) {
        super(maxVersions, start, stop);
        this.family = family;
        this.qualifier = qualifier;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("family", Bytes.toStringBinary(this.family))
            .add("qualifier", Bytes.toStringBinary(this.qualifier))
            .add("maxVersions", getMaxVersions())
            .add("start", getStartTimestamp())
            .add("stop", getStopTimestamp())
            .toString();
    }

    public byte[] getFamily() {
        return this.family;
    }

    public byte[] getQualifier() {
        return this.qualifier;
    }

    @Override
    protected boolean includeFamilyAndQualifier(KeyValue keyValue) {
        // include the key/value if it matches the family/qualifer we care about
        return keyValue.matchingColumn(this.family, this.qualifier);
    }

    @Override
    protected boolean resetVersionCount(KeyValue keyValue) {
        // resetting the version count for each row is sufficient, so do no extra work
        return false;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        Bytes.writeByteArray(out, this.family);
        Bytes.writeByteArray(out, this.qualifier);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.family = Bytes.readByteArray(in);
        this.qualifier = Bytes.readByteArray(in);
    }
}
