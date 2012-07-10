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
  private val parser = new QueryParser(this)

  private var tableName : Option[String] = None
  private var queryOperation : Option[QueryOperation.Value] = None
  private var columns : List[Column] = Nil
  private var rowConstraints : List[RowConstraint] = Nil

  private var namedParameters : Map[String, Any] = new HashMap[String, Any]

  // Parse the input after all of the private variables have been declared
  this.parser.parseAll(parser.query, query.toLowerCase)

  protected[query] def scan : QueryBuilder = {
    this.queryOperation = Some(QueryOperation.Scan)
    this
  }

  protected[query] def get : QueryBuilder = {
    this.queryOperation = Some(QueryOperation.Get)
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

protected[query] class QueryParser(private val queryBuilder : QueryBuilder) extends RegexParsers {
  def query = selectClause ~ fromClause ~ whereClause.?

  def selectClause = scanClause | getClause
  def scanClause = "scan" ~> repsep(columnDefinition, ",") ^^ { _ => this.queryBuilder.scan }
  def getClause = "get" ~> repsep(columnDefinition, ",") ^^ { _ => this.queryBuilder.get }

  def columnDefinition : Parser[Column] = versionDefinition.? ~ columnFamily ~ ":" ~ columnQualifier ~ timeRange.? ^^ {
    case versions ~ family ~ ":" ~ qualifier ~ time => {
      val column = Column(Bytes.toBytesBinary(family),
                          Bytes.toBytesBinary(qualifier),
                          versions.getOrElse(QueryVersions.One),
                          time)
      this.queryBuilder.addColumnDefinition(column)
      column
    }
  }

  def versionDefinition : Parser[QueryVersions] = allVersions | someVersions

  def allVersions : Parser[QueryVersions] = "all" ~ "versions" ~ "of" ^^ { _ => QueryVersions.All }

  def someVersions : Parser[QueryVersions] = positiveWholeNumber <~ "versions?".r <~ "of" ^^ { n => QueryVersions(n.toInt) }

  /** Column family name must consist of printable characters. Regex \w is pretty close */
  def columnFamily : Parser[String] = """\w+""".r

  /**
   * Column qualifier can be any bytes, so we accept characters that come out of a call to
   * {@link org.apache.hadoop.hbase.util.Bytes#toStringBinary}.  This means, users must use
   * {@link org.apache.hadoop.hbase.util.Bytes#toStringBinary} to translate qualifiers that are
   * complex byte arrays into a String before constructing the query
   */
  def columnQualifier : Parser[String] = """([a-zA-Z0-9 `~!@#$%^&*()\-_=+\[\]\{\}\\|;:'",.<>/?]|(\\x[0-9]{2}))+""".r

  def timeRange : Parser[(String, String)] = "between" ~ parameter ~ "and" ~ parameter ^^ {
    case _ ~ a ~ _ ~ b => (a, b)
  }

  def fromClause : Parser[String] = "from" ~> tableName ^^ { t =>
    this.queryBuilder.setTableName(t)
    t
  }

  /**
   * Table names must not start with a '.' or a '-' and may only contain Latin letters or numbers
   * as well as '_', '-', or '.'.
   */
  def tableName : Parser[String] = """\w[\w\-.]*""".r

  def whereClause : Parser[RowConstraint] = "where" ~> rowKeyConstraint ^^ { c =>
    this.queryBuilder.addConstraint(c)
    c
  }

  // TODO: Support "rowkey between X and Y" syntax
  def rowKeyConstraint : Parser[RowConstraint] = "rowkey" ~> rowKeyOperator ~ parameter ^^ {
    case o ~ p => RowConstraint(o, p)
  }
  // Longer patterns must come first (ie, <= and >= before < and >) so that the match can be greedy
  def rowKeyOperator : Parser[String] = "<=" | ">=" | "<" | ">" | "="

  /** All named parameters are word characters enclosed with { and } */
  def parameter : Parser[String] = "{" ~> """\w*""".r <~ "}"

  def positiveWholeNumber : Parser[String] = """[1-9]\d*""".r
}
