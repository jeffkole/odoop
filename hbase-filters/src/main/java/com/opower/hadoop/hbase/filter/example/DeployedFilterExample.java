package com.opower.hadoop.hbase.filter.example;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.opower.hadoop.hbase.filter.DeployedFilter;
import com.opower.hadoop.hbase.filter.DeployedFilterManager;
import com.opower.hadoop.hbase.filter.RowKeyInSetFilter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Shows how one could use a {@link DeployedFilter}.
 *
 * @author jeff@opower.com
 */
public final class DeployedFilterExample {
    private DeployedFilterExample() {}

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        DeployedFilterManager filterManager =
            new DeployedFilterManager(configuration);

        // Just some cheap and easy command-line parsing.  The first
        // argument is the name of the table to use, and the rest are
        // row keys to use for the RowKeyInSetFilter
        List<String> argList = new LinkedList<String>(Arrays.asList(args));
        String tableName = argList.get(0);
        argList.remove(0);

        RowKeyInSetFilter wrappedFilter = new RowKeyInSetFilter(argList);
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
}
