package com.opower.hadoop.hbase.query;

import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

/**
 * FILL THIS IN!
 *
 * @author jeff@opower.com
 */
public class DefaultQueryPlanner implements QueryPlanner {
    /**
     * {@inheritDoc}
     */
    @Override
    public Query parse(String query) {
        QueryBuilder builder = QueryBuilder.parse(query);
        Scan scan = new Scan();
        List<Column> columns = builder.getColumnList();
        for (Column column : columns) {
            scan.addColumn(column.family(), column.qualifier());
        }
        List<RowConstraint> constraints = builder.getRowConstraintList();
        if (!constraints.isEmpty()) {

        }
        return null;
    }
}
