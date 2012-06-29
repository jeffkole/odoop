package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Acts as a wrapper around another {@link Filter} allowing for that filter to be dynamically deployed
 * into an HBase cluster.  All of the {@link Filter} methods just delegate to the wrapped filter.  The
 * interesting work occurs during deserialization on the region server, when the
 * {@link DeployedFilterManager#loadFilter} method is called and the wrapped filter is loaded dynamically.
 *
 * @author jeff@opower.com
 */
public class DeployedFilter implements Filter {
    private Path remoteJarPath;
    private Filter wrappedFilter;
    private String wrappedFilterName;

    /**
     * Required default constructor for serialization
     */
    public DeployedFilter() {}

    /**
     * Constructor meant to be used only by the {@link DeployedFilterManager}
     *
     * @param remoteJarPath a Path to the jar in HDFS that holds the class definition for the wrapped filter
     * @param wrappedFilter the {@link Filter} to be deployed
     */
    DeployedFilter(Path remoteJarPath, Filter wrappedFilter) {
        this.remoteJarPath = remoteJarPath;
        this.wrappedFilterName = wrappedFilter.getClass().getName();
        this.wrappedFilter = wrappedFilter;
    }

    /**
     * Meant to be used by the {@link DeployedFilterManager} during the undeploying phase
     *
     * @return the Path to the jar in HDFS
     */
    Path getRemoteJarPath() {
        return this.remoteJarPath;
    }

    public void reset() {
        this.wrappedFilter.reset();
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        return this.wrappedFilter.filterRowKey(buffer, offset, length);
    }

    public boolean filterAllRemaining() {
        return this.wrappedFilter.filterAllRemaining();
    }

    public ReturnCode filterKeyValue(KeyValue v) {
        return this.wrappedFilter.filterKeyValue(v);
    }

    public void filterRow(List<KeyValue> kvs) {
        this.wrappedFilter.filterRow(kvs);
    }

    public boolean hasFilterRow() {
        return this.wrappedFilter.hasFilterRow();
    }

    public boolean filterRow() {
        return this.wrappedFilter.filterRow();
    }

    public KeyValue getNextKeyHint(KeyValue currentKV) {
        return this.wrappedFilter.getNextKeyHint(currentKV);
    }

    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, Bytes.toBytes(this.remoteJarPath.toString()));
        Bytes.writeByteArray(out, Bytes.toBytes(this.wrappedFilterName));
        this.wrappedFilter.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.remoteJarPath = new Path(Bytes.toString(Bytes.readByteArray(in)));
        this.wrappedFilterName = Bytes.toString(Bytes.readByteArray(in));
        this.wrappedFilter = DeployedFilterManager.loadFilter(this.remoteJarPath, this.wrappedFilterName);
        this.wrappedFilter.readFields(in);
    }
}
