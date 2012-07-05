package com.opower.hadoop.hbase.query

import scala.util.parsing.combinator.RegexParsers

class QueryParser extends RegexParsers {
  def query = selectClause ~ fromClause ~ whereClause.?

  def selectClause = scanClause | getClause
  def scanClause = "scan" ~> repsep(columnDefinition, ",")
  def getClause = "get" ~> repsep(columnDefinition, ",")

  def columnDefinition = versionDefinition.? ~ columnFamily ~ ":" ~ columnQualifier ~ timeRange.?
  def versionDefinition = allVersions | someVersions
  def allVersions = "all" ~ "versions" ~ "of"
  def someVersions = positiveWholeNumber <~ "versions?".r <~ "of"
  /** Column family name must consist of printable characters. Regex \w is pretty close */
  def columnFamily = """\w+""".r
  /** Column qualifier can be any bytes, so we accept word characters (\w) or hex-encoded byte literals */
  def columnQualifier = """(\w|0x[0-9]{2})+""".r
  def timeRange = "between" ~ parameter ~ "and" ~ parameter

  def fromClause = "from" ~> tableName
  /**
   * Table names must not start with a '.' or a '-' and may only contain Latin letters or numbers
   * as well as '_', '-', or '.'.
   */
  def tableName = """\w[\w\-.]*""".r

  def whereClause = "where" ~> rowKeyConstraint
  // TODO: Support "rowkey between X and Y" syntax
  def rowKeyConstraint = "rowkey" ~> rowKeyOperator ~ parameter
  def rowKeyOperator = "([<>]=?|=)".r

  /** All named parameters are word characters enclosed with { and } */
  def parameter = "{" ~> """\w*""".r <~ "}"

  def positiveWholeNumber = """[1-9]\d*""".r
}
