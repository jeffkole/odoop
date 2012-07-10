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

case class Column(family    : Array[Byte]              = Array[Byte](),
                  qualifier : Array[Byte]              = Array[Byte](),
                  versions  : QueryVersions            = QueryVersions.One,
                  timeRange : Option[(String, String)] = None) {

  /**
   * Judges equality based on the deep equality of the family and qualifier as well as
   * the versions and time range
   *
   * {@inheritDoc}
   */
  override def equals(other : Any) : Boolean = {
    if (other == null) {
      return false
    }
    if (!other.isInstanceOf[Column]) {
      return false
    }
    val that = other.asInstanceOf[Column]
    return (this.family.deep.equals(that.family.deep) &&
      this.qualifier.deep.equals(that.qualifier.deep) &&
      this.versions.equals(that.versions) &&
      this.timeRange.equals(that.timeRange))
  }

  /**
   * Computes hashCode based on the deep equality of the family and qualifier as well as
   * the versions and time range
   *
   * {@inheritDoc}
   */
  override def hashCode : Int = {
    var result = 17
    result = 31 * result + this.family.deep.hashCode
    result = 31 * result + this.qualifier.deep.hashCode
    result = 31 * result + this.versions.hashCode
    result = 31 * result + this.timeRange.hashCode
    result
  }

  override def toString : String = {
    "Column(%s, %s, %s, %s)".format(
      Bytes.toStringBinary(this.family),
      Bytes.toStringBinary(this.qualifier),
      this.versions,
      this.timeRange)
  }
}

class QueryEngine {
  def parse(query : String) : QueryBuilder = {
    val builder = new QueryBuilder(query)
    val parser = new QueryParser(builder)
    parser.parseAll(parser.query, query.toLowerCase)
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
  def query : Parser[~[~[List[Column], String], Option[RowConstraint]]] = scanClause ~ fromClause ~ whereClause.?

  def scanClause : Parser[List[Column]] = "scan" ~> repsep(columnDefinition, ",") ^^ { q =>
    this.queryBuilder.scan
    q
  }

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
  // TODO: handle commas in the qualifier name, which currently break parsing done by repsep(columnDefinition, ",")
  // TODO: handle spaces in the qualifier name, which break parsing the rest of the line
  def columnQualifier : Parser[String] = """([a-zA-Z0-9`~!@#$%^&*()\-_=+\[\]\{\}\\|;:'".<>/?]|(\\x[0-9]{2}))+""".r

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
