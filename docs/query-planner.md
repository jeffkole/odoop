---
layout: doc
title: Query Planner
---

### Query Grammar

The currently implemented grammar is as follows (case sensitive):

    query             := selectClause fromClause [ whereClause ]
    selectClause      := scanClause
    fromClause        := "from" tableName
    whereClause       := "where" rowKeyConstraint
    scanClause        := "scan" [ column { "," column } ]
    column            := [ version ] family ":" qualifier [ timeRange ]
    version           := "all versions of" | positiveNumber "versions of"
    family            := /\w+/
    qualifier         := literal | literal "*" | "*"
    literal           := /([a-zA-Z0-9`~!@#$%^&()\-_=+\[\]\{\}\\|;:'".<>/?]|(\\x[0-9]{2}))+/
    timeRange         := "between" parameter "and" parameter
    tableName         := /\w[\w\-.]*/
    rowKeyConstraint  := ( "rowkey" rowKeyOperator parameter | "rowkey between" parameter "and" parameter )
    rowKeyOperator    := "<" | "<=" | ">" | ">=" | "="
    parameter         := "{" /\w*/ "}"
    positiveNumber    := /[1-9]\d*/


The data model for the example queries is roughly something like this:

HBase table named `customer` with a single column family: `d`.  The row key is a synthetic primary key for a customer.
Columns include:

* address: multiple versions to track a customer moving
* clicks: time series of clicks the customer has made on a website
* predictions: an expandable column prefix that results in multiple columns, kind of like a map

### Example Queries

    scan d:address,
         all versions of d:clicks between {start} and {stop}
    from customer
    where rowkey = {id}

Additional query functionality can include pattern matching at the column level like the following.

Fetch the four most recent versions of all columns from the `d` family:

    scan 4 versions of d:* ...

Fetch the most recent version of all predictions:

    scan d:preditions* ...

Additional constraints can be added to the where clause as well.  This example shows a literal parameter value just
for ease of understanding.  Literal values are not allowed; all parameters must be demarcated with `{` and `}` and then
set according to the parameter name.

Assuming a click is deserialized as `<target URL>|<referrer URL>`, fetch all clicks coming from http://google.com:

    scan all versions of d:clicks
    from customer
    where d:clicks =~ /.*\|http://google.com/

Fetch all customers between IDs 50 and 100, inclusive at the beginning, exclusive at the end:

    scan * from customer where rowkey between 50 and 100

### Example Usage

    QueryPlanner planner = new QueryPlanner(new HTablePool(HBaseConfiguration.create()));
    Query query = planner.parse("scan from customer where rowkey = {id}");
    query.setInt("id", 42);
    ResultScanner scanner = query.scan();
    for (Result result : scanner) {
        // do some real work
    }
    scanner.close();
    query.close();
    planner.close();

The QueryPlanner is fully responsible for constructing an efficient Scan and managing the HTable and its connections.
