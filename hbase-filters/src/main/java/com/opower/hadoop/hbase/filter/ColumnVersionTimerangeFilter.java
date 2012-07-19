package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
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
public class ColumnVersionTimerangeFilter extends FilterBase {
    private byte[] family;
    private byte[] qualifier;
    private int numVersions;
    private long start;
    private long stop;

    private int currentNumVersionsFound = 0;

    public ColumnVersionTimerangeFilter() {}

    /**
     * Create a filter based on the family, qualifier, and maximum number of versions to
     * return.  The timerange will be effectiely ignored.
     *
     * @param family
     * @param qualifier
     * @param numVersions
     */
    public ColumnVersionTimerangeFilter(byte[] family, byte[] qualifier, int numVersions) {
        this(family, qualifier, numVersions, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Create a filter based on the family, qualifier, maximum number of versions, and a
     * timerange to check.  The timerange is inclusive at the beginning and exclusive on
     * the end: {@code [start, stop)}.
     *
     * @param family
     * @param qualifier
     * @param numVersions
     * @param start
     * @param stop
     */
    public ColumnVersionTimerangeFilter(byte[] family, byte[] qualifier, int numVersions, long start, long stop) {
        this.family = family;
        this.qualifier = qualifier;
        this.numVersions = numVersions;
        this.start = start;
        this.stop = stop;
    }

    @Override
    public void reset() {
        this.currentNumVersionsFound = 0;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        // If this filter has nothing to do with this key/value, then skip it.
        // If this filter is included in a FilterList, there is still the opportunity
        // for another filter to include the key/value.
        if (!keyValue.matchingColumn(this.family, this.qualifier)) {
            return ReturnCode.SKIP; // TODO: should this be NEXT_COL, or does that mess with FilterList?
        }
        // If we have found enough versions of this qualifier, then skip.
        if (this.currentNumVersionsFound >= this.numVersions) {
            return ReturnCode.SKIP; // TODO: should this be NEXT_COL, or does that mess with FilterList?
        }
        long timestamp = keyValue.getTimestamp();
        if (this.start <= timestamp && timestamp < stop) {
            this.currentNumVersionsFound++;
            return ReturnCode.INCLUDE;
        }
        return ReturnCode.SKIP;
    }

    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, this.family);
        Bytes.writeByteArray(out, this.qualifier);
        out.writeInt(this.numVersions);
        out.writeLong(this.start);
        out.writeLong(this.stop);
    }

    public void readFields(DataInput in) throws IOException {
        this.family = Bytes.readByteArray(in);
        this.qualifier = Bytes.readByteArray(in);
        this.numVersions = in.readInt();
        this.start = in.readLong();
        this.stop = in.readLong();
    }
}
