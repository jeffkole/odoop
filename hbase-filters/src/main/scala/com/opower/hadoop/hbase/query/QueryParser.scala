package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.util.parsing.combinator.RegexParsers

object QueryOperation extends Enumeration {
  val Scan, Get = Value
}

case class QueryVersions(numVersions : Int)

object QueryVersions {
  val All = new QueryVersions(Int.MaxValue)
    val One = new QueryVersions(1)
  }

case class RowConstraint(operator : String, parameter : String)

case class Column(family : Array[Byte],
  qualifier : Array[Byte],
  versions : QueryVersions,
  timeRange : Option[(String, String)])

class QueryBuilder(query : String) {
  private val parser = new QueryParser(this, query)

  private var tableName : Option[String] = None
  private var queryOperation : Option[QueryOperation.Value] = None
  private var columns : List[Column] = Nil
  private var rowConstraints : List[RowConstraint] = Nil

  private var namedParameters : Map[String, Any] = new HashMap[String, Any]

  // Parse the input after all of the private variables have been declared
  this.parser.parseAll(parser.query, query.toLowerCase)

  private def scan = { this.queryOperation = Some(QueryOperation.Scan) }

  private def get = { this.queryOperation = Some(QueryOperation.Get) }

  private def setTableName(tableName : String) = { this.tableName = Some(tableName) }

  private def addColumnDefinition(version : Option[QueryVersions],
    family : String,
    qualifier : String,
    timeRange : Option[(String, String)]) = {
    // TODO: handle hex-encoded bytes in the qualifier
    val column = Column(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier),
                        version.getOrElse(QueryVersions.One),
                        timeRange)
    for ((startName, stopName) <- timeRange) {
      if (this.namedParameters.contains(startName)) {
        throw new RuntimeException("startName %s exists".format(startName))
      }
      this.namedParameters.put(startName, column)
      if (this.namedParameters.contains(stopName)) {
        throw new RuntimeException("stopName %s exists".format(stopName))
      }
      this.namedParameters.put(stopName, column)
    }
    this.columns = column :: this.columns
  }

  private def addConstraint(constraint : RowConstraint) = {
    this.rowConstraints = constraint :: this.rowConstraints
  }

  override def toString = {
    "Operation: %s; Table: %s; Columns: %s; Row constraints: %s".format(
      this.queryOperation, this.tableName, this.columns, this.rowConstraints)
  }

  private class QueryParser(private val queryBuilder : QueryBuilder, private val input : String) extends RegexParsers {
    def query = selectClause ~ fromClause ~ whereClause.?

    def selectClause = scanClause | getClause
    def scanClause = "scan" ~> repsep(columnDefinition, ",") ^^ { _ => this.queryBuilder.scan }
    def getClause = "get" ~> repsep(columnDefinition, ",") ^^ { _ => this.queryBuilder.get }

    def columnDefinition = versionDefinition.? ~ columnFamily ~ ":" ~ columnQualifier ~ timeRange.? ^^ {
      case v ~ f ~ ":" ~ q ~ t => this.queryBuilder.addColumnDefinition(v, f, q, t)
    }
    def versionDefinition = allVersions | someVersions
    def allVersions : Parser[QueryVersions] = "all" ~ "versions" ~ "of" ^^ { _ => QueryVersions.All }
    def someVersions : Parser[QueryVersions] = positiveWholeNumber <~ "versions?".r <~ "of" ^^ { n => QueryVersions(n.toInt) }
    /** Column family name must consist of printable characters. Regex \w is pretty close */
    def columnFamily = """\w+""".r
    /** Column qualifier can be any bytes, so we accept word characters (\w) or hex-encoded byte literals */
    def columnQualifier = """(\w|0x[0-9a-fA-F]{2})+""".r
    def timeRange = "between" ~ parameter ~ "and" ~ parameter ^^ {
      case _ ~ a ~ _ ~ b => (a, b)
    }

    def fromClause = "from" ~> tableName ^^ { t => this.queryBuilder.setTableName(t) }
    /**
     * Table names must not start with a '.' or a '-' and may only contain Latin letters or numbers
     * as well as '_', '-', or '.'.
     */
    def tableName = """\w[\w\-.]*""".r

    def whereClause = "where" ~> rowKeyConstraint ^^ { c => this.queryBuilder.addConstraint(c) }
    // TODO: Support "rowkey between X and Y" syntax
    def rowKeyConstraint = "rowkey" ~> rowKeyOperator ~ parameter ^^ {
      case o ~ p => RowConstraint(o, p)
    }
    // TODO: how should the individual operators be parsed out?
    def rowKeyOperator = "<" | "<=" | ">" | ">=" | "="

    /** All named parameters are word characters enclosed with { and } */
    def parameter = "{" ~> """\w*""".r <~ "}"

    def positiveWholeNumber = """[1-9]\d*""".r
  }
}
