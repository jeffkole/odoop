Query Planner
=============

The data model for the example queries is roughly something like this:

HBase table named `customer` with a single column family: `d`.  The row key is a synthetic primary key for a customer.
Columns include:

* address: multiple versions to track a customer moving
* clicks: time series of clicks the customer has made on a website
* predictions: an expandable column prefix that results in multiple columns, kind of like a map

Example Queries
===============

```
scan d:address,
     all versions of d:clicks between 2012-06-01T00:00:00 and 2012-07-01T00:00:00
from customer
where rowkey = 42
```

Additional query functionality can include pattern matching at the column level like the following.

Fetch the four most recent versions of all columns from the `d` family:
```
scan 4 versions of d:* ...
```

Fetch the most recent version of all predictions:
```
scan d:preditions* ...
```

Additional constraints can be added to the where clause as well.

Assuming a click is deserialized as "<target URL>|<referrer URL>", fetch all clicks coming from http://google.com:
```
scan all versions of d:clicks
from customer
where d:clicks =~ /.*\|http://google.com/
```

Fetch all customers between IDs 50 and 100, exclusive:
```
scan * from customer where rowkey between 50 and 100
```

Example Usage
=============

```
QueryPlanner planner = new QueryPlanner(HBaseConfiguration.create());
ResultScanner scanner = planner.scan("scan * from customer where rowkey = 42")
for (Result result : scanner) {
    // do some real work
}
scanner.close();
planner.close();
```

The QueryPlanner is fully responsible for constructing an efficient Scan and managing the HTable and its connections.
