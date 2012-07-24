package com.opower.hadoop.hbase.filter;

import com.google.common.base.Objects;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract base class for filtering a column based on a timerange and number of versions,
 * specifically meant to implement the column selection functionality of the Query Planner.
 *
 * @author jeff@opower.com
 */
public abstract class AbstractColumnVersionTimerangeFilter extends FilterBase {
    private int maxVersions;
    private long start;
    private long stop;

    private int currentNumVersionsFound = 0;

    protected AbstractColumnVersionTimerangeFilter() {}

    /**
     * Create a filter based on the maximum number of versions with a maximal timerange.
     *
     * @param maxVersions
     */
    protected AbstractColumnVersionTimerangeFilter(int maxVersions) {
        this(maxVersions, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * Create a filter based on the maximum number of versions and a timerange to check.
     * The timerange is inclusive at the beginning and exclusive on the end:
     * {@code [start, stop)}.
     *
     * @param maxVersions
     * @param start
     * @param stop
     */
    protected AbstractColumnVersionTimerangeFilter(int maxVersions, long start, long stop) {
        this.maxVersions = maxVersions;
        this.start = start;
        this.stop = stop;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("maxVersions", this.maxVersions)
            .add("start", this.start)
            .add("stop", this.stop)
            .toString();
    }

    public int getMaxVersions() {
        return this.maxVersions;
    }

    public long getStartTimestamp() {
        return this.start;
    }

    public long getStopTimestamp() {
        return this.stop;
    }

    @Override
    public final void reset() {
        resetCurrentVersionCount();
    }

    protected final void resetCurrentVersionCount() {
        this.currentNumVersionsFound = 0;
    }

    /**
     * Determine if the {@link KeyValue} should be included based on its family
     * and qualifier.  Further checks for maximum number of versions and the
     * timerange will be done after this method is called, assuming it returns
     * {@code true}.  If this method returns {@code false}, then the {@code KeyValue}
     * will be skipped.
     *
     * @param keyValue the {@code KeyValue} to check if it should be included in the results
     * @return true if it should be included, false otherwise
     */
    protected abstract boolean includeFamilyAndQualifier(KeyValue keyValue);

    /**
     * Determine if the count of the current number of versions for the current
     * {@code KeyValue} should be reset.
     *
     * @param keyValue the {@code KeyValue} under inspection
     * @return true if the version count should be reset, false otherwise
     */
    protected abstract boolean resetVersionCount(KeyValue keyValue);

    @Override
    public final ReturnCode filterKeyValue(KeyValue keyValue) {
        // If this filter has nothing to do with this key/value, then skip it.
        // If this filter is included in a FilterList, there is still the opportunity
        // for another filter to include the key/value.
        if (!includeFamilyAndQualifier(keyValue)) {
            return ReturnCode.SKIP; // TODO: should this be NEXT_COL, or does that mess with FilterList?
        }
        if (resetVersionCount(keyValue)) {
            resetCurrentVersionCount();
        }
        // If we have found enough versions of this qualifier, then skip.
        if (this.currentNumVersionsFound >= this.maxVersions) {
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
        out.writeInt(this.maxVersions);
        out.writeLong(this.start);
        out.writeLong(this.stop);
    }

    public void readFields(DataInput in) throws IOException {
        this.maxVersions = in.readInt();
        this.start = in.readLong();
        this.stop = in.readLong();
    }
}
