package com.opower.hadoop.hbase.query

class QueryEngine {
  def parse(query : String) : QueryBuilder = {
    val builder = new QueryBuilder(query)
    val parser = new QueryParser(builder)
    parser.parseAll(parser.query, query.toLowerCase)
    builder
  }
}
