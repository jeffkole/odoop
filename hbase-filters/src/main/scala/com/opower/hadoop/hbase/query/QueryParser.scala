package com.opower.hadoop.hbase.query

import scala.util.parsing.combinator.RegexParsers

class QueryParser extends RegexParsers {
  val queryBuilder : QueryBuilder = new QueryBuilder

  def query = selectClause ~ fromClause ~ whereClause.?

  def selectClause = scanClause | getClause
  def scanClause = "scan" ~> repsep(columnDefinition, ",") ^^ {
    case _ => queryBuilder.scan
  }
  def getClause = "get" ~> repsep(columnDefinition, ",") ^^ {
    case _ => queryBuilder.get
  }

  def columnDefinition = versionDefinition.? ~ columnFamily ~ ":" ~ columnQualifier ~ timeRange.? ^^ {
    case v ~ f ~ ":" ~ q ~ t => queryBuilder.addColumnDefinition(v, f, q, t)
  }
  def versionDefinition = allVersions | someVersions
  def allVersions = "all" ~ "versions" ~ "of" ^^ { _ => QueryVersions.All }
  def someVersions = positiveWholeNumber <~ "versions?".r <~ "of" ^^ { n => QueryVersions(n.toInt) }
  /** Column family name must consist of printable characters. Regex \w is pretty close */
  def columnFamily = """\w+""".r
  /** Column qualifier can be any bytes, so we accept word characters (\w) or hex-encoded byte literals */
  def columnQualifier = """(\w|0x[0-9]{2})+""".r
  def timeRange = "between" ~ parameter ~ "and" ~ parameter ^^ {
    case _ ~ a ~ _ ~ b => (a, b)
  }

  def fromClause = "from" ~> tableName ^^ { t => queryBuilder.setTableName(t) }
  /**
   * Table names must not start with a '.' or a '-' and may only contain Latin letters or numbers
   * as well as '_', '-', or '.'.
   */
  def tableName = """\w[\w\-.]*""".r

  def whereClause = "where" ~> rowKeyConstraint ^^ { c => queryBuilder.addConstraint(c) }
  // TODO: Support "rowkey between X and Y" syntax
  def rowKeyConstraint = "rowkey" ~> rowKeyOperator ~ parameter ^^ {
    case o ~ p => Constraint(o, p)
  }
  def rowKeyOperator = "([<>]=?|=)".r

  /** All named parameters are word characters enclosed with { and } */
  def parameter = "{" ~> """\w*""".r <~ "}"

  def positiveWholeNumber = """[1-9]\d*""".r

  object QueryOperation extends Enumeration {
    val Scan, Get = Value
  }

  case class QueryVersions(numVersions : Int) {
  }

  object QueryVersions {
    val All = new QueryVersions(Int.MaxValue)
  }

  case class Constraint(operator : String, parameter : String) {
  }

  class QueryBuilder {
    var tableName : Option[String] = None
    var queryOperation : Option[QueryOperation.Value] = None

    def scan = { this.queryOperation = Some(QueryOperation.Scan) }
    def get = { this.queryOperation = Some(QueryOperation.Get) }
    def setTableName(tableName : String) = { this.tableName = Some(tableName) }
    def addColumnDefinition(version : Option[QueryVersions], family : String,
        qualifier : String, timeRange : Option[(String, String)]) = {
      printf("version: %s, fam: %s, qual: %s, timerange: %s%n", version, family, qualifier, timeRange)
    }
    def addConstraint(constraint : Constraint) = {
      printf("constraint: %s%n", constraint)
    }

    override def toString = {
      "Operation: %s; Table: %s".format(this.queryOperation, this.tableName)
    }
  }
}
