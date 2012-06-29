package com.opower.hadoop.hbase.filter.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.opower.hadoop.hbase.filter.DeployedFilter;
import com.opower.hadoop.hbase.filter.DeployedFilterManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Shows how one could use a {@link DeployedFilter}.
 *
 * @author jeff@opower.com
 */
public final class DeployedFilterExample {
    private DeployedFilterExample() {}

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        DeployedFilterManager filterManager = new DeployedFilterManager(configuration);

        // Just some cheap and easy command-line parsing.  The first argument is the name of the
        // table to use, and the rest are row keys to use for the SpecificRowKeyFilter
        List<String> argList = new LinkedList<String>(Arrays.asList(args));
        String tableName = argList.get(0);
        argList.remove(0);

        SpecificRowKeyFilter wrappedFilter = new SpecificRowKeyFilter(argList);
        // DeployedFilterManager will push the jar containing the
        // wrappedFilter to the cluster so that it can be used on the
        // region servers when the DeployedFilter is deserialized.
        // The location of the jar is encoded in the DeployedFiter,
        // which will find the jar and create a new classloader using it
        // to instantiate the wrappedFilter.
        DeployedFilter filter = filterManager.deployFilter(wrappedFilter);

        Scan scan = new Scan();
        scan.setFilter(filter);

        try {
            HTable table = new HTable(configuration, tableName);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                KeyValue[] kvs = result.raw();
                System.out.printf("%15s    %50s%n",
                        Bytes.toStringBinary(kvs[0].getRow()),
                        Bytes.toStringBinary(kvs[0].getValue()));
            }
            scanner.close();
        }
        finally {
            // now be sure to unregister the filter so that its jar can be
            // cleaned up
            filterManager.undeployFilter(filter);
            HConnectionManager.deleteConnection(configuration, true);
        }
    }

    /**
     * An example filter that will match on the row keys that are passed in as constructor parameters
     *
     * @author jeff@opower.com
     */
    public static class SpecificRowKeyFilter extends FilterBase {
        private Set<String> keys;

        // Transient field to track progress through the filter lifecycle
        private boolean rowInSet = false;

        public SpecificRowKeyFilter() {}

        public SpecificRowKeyFilter(Collection<String> rowKeys) {
            this.keys = new HashSet<String>();
            this.keys.addAll(rowKeys);
        }

        @Override
        public void reset() {
            this.rowInSet = false;
        }

        @Override
        public ReturnCode filterKeyValue(KeyValue v) {
            if (!this.rowInSet) {
                return ReturnCode.NEXT_ROW;
            }
            return ReturnCode.INCLUDE;
        }

        @Override
        public boolean filterRow() {
            return !this.rowInSet;
        }

        @Override
        public boolean filterRowKey(byte[] rowKeyBuffer, int offset, int length) {
            String key = Bytes.toString(rowKeyBuffer, offset, length);
            // If the row is found in the set, then return false to continue
            if (this.keys.contains(key)) {
                this.rowInSet = true;
            }
            else {
                // Otherwise, we can skip the rest of processing for the row
                this.rowInSet = false;
            }
            return !rowInSet;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(this.keys.size());
            for (String key : keys) {
                WritableUtils.writeString(out, key);
            }
        }

        public void readFields(DataInput in) throws IOException {
            int numKeys = in.readInt();
            this.keys = new HashSet<String>();
            for (int i = 0; i < numKeys; i++) {
                this.keys.add(WritableUtils.readString(in));
            }
        }
    }
}
