package com.opower.hadoop.hbase.query

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

object QueryBuilder {
  def parse(query : String) : QueryBuilder = {
    val builder = new QueryBuilder(query)
    val parser = new QueryParser(builder)
    parser.parseAll(parser.query, query.toLowerCase) match {
      case parser.Failure(msg, next) => throw new IllegalArgumentException(msg)
      case _ => // success! so carry on
    }
    builder
  }
}

class QueryBuilder(query : String) {
  private var tableName : Option[String] = None
  private var queryOperation : Option[QueryOperation.Value] = None
  private var columns : List[Column] = Nil
  private var rowConstraints : List[RowConstraint] = Nil

  private var namedParameters : Map[String, Any] = new HashMap[String, Any]

  protected[query] def getTableName : Option[String] = {
    this.tableName
  }

  protected[query] def getQueryOperation : Option[QueryOperation.Value] = {
    this.queryOperation
  }

  protected[query] def getColumns : List[Column] = {
    this.columns
  }

  protected[query] def getRowConstraints : List[RowConstraint] = {
    this.rowConstraints
  }

  protected[query] def scan : QueryBuilder = {
    this.queryOperation = Some(QueryOperation.Scan)
    this
  }

  protected[query] def setTableName(tableName : String) : QueryBuilder = {
    this.tableName = Some(tableName)
    this
  }

  protected[query] def addColumnDefinition(column : Column) : QueryBuilder = {
    for ((startName, stopName) <- column.timeRange) {
      this.namedParameters.put(startName, column)
      this.namedParameters.put(stopName, column)
    }
    this.columns = column :: this.columns
    this
  }

  protected[query] def addConstraint(constraint : RowConstraint) : QueryBuilder = {
    this.rowConstraints = constraint :: this.rowConstraints
    this
  }

  override def toString = {
    "Operation: %s; Table: %s; Columns: %s; Row constraints: %s".format(
      this.queryOperation, this.tableName, this.columns, this.rowConstraints)
  }
}
