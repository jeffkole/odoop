package com.opower.hadoop.hbase.query.example;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.opower.hadoop.hbase.query.DefaultQueryPlanner;
import com.opower.hadoop.hbase.query.Query;
import com.opower.hadoop.hbase.query.QueryPlanner;

/**
 * An example program to show how to use the {@link QueryPlanner} and {@link Query}
 * mechanism.
 *
 * @author jeff@opower.com
 */
public final class QueryExample {
    private QueryExample() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.printf("usage: %s <query> [<name>=<value>]*%n", QueryExample.class.getName());
            System.exit(1);
        }

        String queryString = args[0];

        Configuration configuration = HBaseConfiguration.create();
        HTablePool pool = new HTablePool(configuration, 1);
        QueryPlanner planner = new DefaultQueryPlanner(pool);
        Query query = planner.parse(queryString);
        if (args.length > 1) {
            for (int i = 1; i < args.length; i++) {
                String[] nameValue = args[i].split("=");
                if (nameValue[0].startsWith("ts")) {
                    query.setTimestamp(nameValue[0], Long.parseLong(nameValue[1]));
                }
                else {
                    query.setString(nameValue[0], nameValue[1]);
                }
            }
        }
        ResultScanner scanner = query.scan();
        for (Result result : scanner) {
            for (KeyValue keyValue : result.raw()) {
                System.out.printf("%15s   %s:%s(@%d)=%50s%n",
                        Bytes.toStringBinary(keyValue.getRow()),
                        Bytes.toStringBinary(keyValue.getFamily()),
                        Bytes.toStringBinary(keyValue.getQualifier()),
                        keyValue.getTimestamp(),
                        Bytes.toStringBinary(keyValue.getValue()));
            }
        }
        scanner.close();
        query.close();
        planner.close();
        pool.close();
    }
}
