package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.immutable

object QueryBuilder {
  def parse(query : String) : QueryBuilder = {
    val builder = new QueryBuilder(query)
    val parser = new QueryParser(builder)
    parser.parseAll(parser.query, query.toLowerCase) match {
      case f : parser.Failure => throw new IllegalArgumentException(f.toString)
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

  private var namedParameters : mutable.Map[String, Any] = new mutable.HashMap[String, Any]

  /**
   * Construct a {@link Scan} given the internal state of the builder and the query parameters
   * and timestamps.  This method exists for interoperability with Java, so the parameter types
   * are Java types.  The meat of the work occurs in {@link #doPlanScan}, which takes native
   * Scala types.
   */
  protected[query] def planScan(parameters : java.util.Map[String, Array[Byte]],
                                timestamps : java.util.Map[String, java.lang.Long]) : Scan = {
    val paramBuilder = Map.newBuilder[String, Array[Byte]]
    paramBuilder ++= parameters.asScala
    val timestampBuilder = Map.newBuilder[String, Long]
    timestampBuilder ++= timestamps.asScala.asInstanceOf[TraversableOnce[(String, Long)]]

    this.doPlanScan(paramBuilder.result, timestampBuilder.result)
  }

  protected[query] def doPlanScan(parameters : immutable.Map[String, Array[Byte]],
                                  timestamps : immutable.Map[String, Long]) : Scan = {
    val scan = new Scan
    for (column <- this.columns) {
      scan.addColumn(column.family, column.qualifier)
    }
    scan
  }

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
