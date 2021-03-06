package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.InclusiveStopFilter
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.immutable

import com.opower.hadoop.hbase.filter.ColumnVersionTimerangeFilter
import com.opower.hadoop.hbase.filter.FamilyOnlyColumnVersionTimerangeFilter
import com.opower.hadoop.hbase.filter.QualifierPrefixColumnVersionTimerangeFilter

object QueryBuilder {
  def parse(query : String) : QueryBuilder = {
    val builder = new QueryBuilder(query)
    val parser = new QueryParser(builder)
    parser.parseAll(parser.query, query) match {
      case f : parser.Failure => throw new IllegalArgumentException(f.toString)
      case _ => // success! so carry on
    }
    builder
  }
}

class QueryBuilder(query : String) {
  private val zeroByte = Array[Byte](0x0)

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
    validateParameters(parameters)
    validateTimestamps(timestamps)

    val scan = new Scan

    val columnFilterBuilder = Seq.newBuilder[Filter]
    val allColumnsHaveTheSameNumVersions = this.columns.map(_.versions.numVersions).distinct.length == 1
    val allColumnsHaveTimeRanges = !this.columns.exists(_.timeRange.isEmpty)
    val anyColumnsHaveTimeRanges = this.columns.exists(!_.timeRange.isEmpty)
    val anyColumnsHaveQualifierPrefixes = this.columns.exists(_.qualifier.isInstanceOf[PrefixQualifier])
    val filterRequired = !allColumnsHaveTheSameNumVersions || anyColumnsHaveTimeRanges || anyColumnsHaveQualifierPrefixes
    val familiesWithQualifierPrefixes = this.columns.filter(_.qualifier.isInstanceOf[PrefixQualifier]).map(_.family)

    for (column <- this.columns) {
      implicit def string2BytesBinary(string : String) : Array[Byte] = Bytes.toBytesBinary(string)
      column match {
        case Column(f, StandardQualifier(q), QueryVersions(n), Some((a, b))) => {
          // if any column has a qualifier prefix in the same family, then only add the family
          if (familiesWithQualifierPrefixes.contains(f)) {
            scan.addFamily(f)
          }
          else {
            scan.addColumn(f, q)
          }
          columnFilterBuilder += new ColumnVersionTimerangeFilter(f, q, n, timestamps(a), timestamps(b))
        }
        case Column(f, StandardQualifier(q), QueryVersions(n), None) => {
          // if any column has a qualifier prefix in the same family, then only add the family
          if (familiesWithQualifierPrefixes.contains(f)) {
            scan.addFamily(f)
          }
          else {
            scan.addColumn(f, Bytes.toBytesBinary(q))
          }
          if (filterRequired) {
            columnFilterBuilder += new ColumnVersionTimerangeFilter(f, q, n)
          }
        }
        case Column(f, PrefixQualifier(q), QueryVersions(n), Some((a, b))) => {
          scan.addFamily(f)
          columnFilterBuilder += new QualifierPrefixColumnVersionTimerangeFilter(f, q, n, timestamps(a), timestamps(b))
        }
        case Column(f, PrefixQualifier(q), QueryVersions(n), None) => {
          scan.addFamily(f)
          columnFilterBuilder += new QualifierPrefixColumnVersionTimerangeFilter(f, q, n )
        }
        case Column(f, EmptyPrefixQualifier(), QueryVersions(n), Some((a, b))) => {
          scan.addFamily(f)
          columnFilterBuilder += new FamilyOnlyColumnVersionTimerangeFilter(f, n, timestamps(a), timestamps(b))
        }
        case Column(f, EmptyPrefixQualifier(), QueryVersions(n), None) => {
          scan.addFamily(f)
          if (filterRequired) {
            columnFilterBuilder += new FamilyOnlyColumnVersionTimerangeFilter(f, n)
          }
        }
        case Column(_, EmptyQualifier(), _, _) => {
          throw new IllegalArgumentException("Cannot handle empty column qualifiers")
        }
      }
    }
    val columnFilters = columnFilterBuilder.result

    if (!this.columns.isEmpty) {
      // pull out the min and max timestamps in order to set the timerange on the scan
      if (!timestamps.isEmpty) {
        // only set a timerange on the scan if all columns have timeranges defined themselves
        if (allColumnsHaveTimeRanges) {
          val minTimestamp = timestamps.minBy(_._2)._2
          val maxTimestamp = timestamps.maxBy(_._2)._2
          scan.setTimeRange(minTimestamp, maxTimestamp)
        }
      }
      // set the max versions to max requested of all columns
      scan.setMaxVersions(this.columns.maxBy(_.versions.numVersions).versions.numVersions)
    }

    var rowFilter : Option[Filter] = None
    for (rowConstraint <- this.rowConstraints) {
      rowConstraint match {
        case SingleRowConstraint(">=", paramName) => scan.setStartRow(parameters(paramName))
        case SingleRowConstraint("<",  paramName) => scan.setStopRow(parameters(paramName))
        case SingleRowConstraint("=",  paramName) => {
          val startRow = parameters(paramName)
          val stopRow = Bytes.add(startRow, zeroByte)
          scan.setStartRow(startRow)
          scan.setStopRow(stopRow)
        }
        case SingleRowConstraint(">",  paramName) => {
          val startRow = parameters(paramName)
          scan.setStartRow(startRow)
          rowFilter = Some(new RowFilter(CompareOp.GREATER, new BinaryComparator(startRow)))
        }
        case SingleRowConstraint("<=", paramName) => rowFilter = Some(new InclusiveStopFilter(parameters(paramName)))
        case BetweenRowConstraint(startParamName, stopParamName) => {
          scan.setStartRow(parameters(startParamName))
          scan.setStopRow(parameters(stopParamName))
        }
      }
    }

    if (!columnFilters.isEmpty) {
      val columnFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, columnFilters.asJava)
      if (!rowFilter.isEmpty) {
        val scanFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, Seq(rowFilter.get, columnFilter).asJava)
        scan.setFilter(scanFilter)
      }
      else {
        scan.setFilter(columnFilter)
      }
    }
    else if (!rowFilter.isEmpty) {
      scan.setFilter(rowFilter.get)
    }

    scan
  }

  private def validateParameters(parameters : immutable.Map[String, Array[Byte]]) : Unit = {
    for (rowConstraint <- this.rowConstraints) {
      rowConstraint match {
        case SingleRowConstraint(_, paramName) => {
          if (!parameters.contains(paramName)) {
            throw new IllegalArgumentException("Missing parameter '%s' for rowkey constraint".format(paramName))
          }
        }
        case BetweenRowConstraint(a, b) => {
          if (!parameters.contains(a)) {
            throw new IllegalArgumentException("Missing parameter '%s' for rowkey constraint".format(a))
          }
          if (!parameters.contains(b)) {
            throw new IllegalArgumentException("Missing parameter '%s' for rowkey constraint".format(b))
          }
        }
      }
    }
  }

  private def validateTimestamps(timestamps : immutable.Map[String, Long]) : Unit = {
    for (column <- this.columns) {
      column match {
        case Column(_, _, _, Some((a, b))) => {
          if (!timestamps.contains(a)) {
            throw new IllegalArgumentException("Missing timestamp parameter '%s' for column constraint".format(a))
          }
          if (!timestamps.contains(b)) {
            throw new IllegalArgumentException("Missing timestamp parameter '%s' for column constraint".format(b))
          }
        }
        case Column(_, _, _, None) => // If the timerange is not specified in the column, then all is good
      }
    }
  }

  protected[query] def getTableName : String = {
    this.tableName.orNull
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
