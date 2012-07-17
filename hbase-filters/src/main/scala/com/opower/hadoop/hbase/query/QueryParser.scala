package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.util.Bytes

import scala.util.parsing.combinator.RegexParsers

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

  def rowKeyConstraint : Parser[RowConstraint] = "rowkey" ~> (
    ("between" ~ parameter ~ "and" ~ parameter ^^ {
        case _ ~ a ~ _ ~ b => BetweenRowConstraint(a, b)
      }) |
    (rowKeyOperator ~ parameter ^^ {
        case o ~ p => SingleRowConstraint(o, p)
      }))

  // Longer patterns must come first (ie, <= and >= before < and >) so that the match can be greedy
  def rowKeyOperator : Parser[String] = "<=" | ">=" | "<" | ">" | "="

  /** All named parameters are word characters enclosed with { and } */
  def parameter : Parser[String] = "{" ~> """\w*""".r <~ "}"

  def positiveWholeNumber : Parser[String] = """[1-9]\d*""".r
}
