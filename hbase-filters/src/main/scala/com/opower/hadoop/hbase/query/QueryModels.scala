package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.util.Bytes

/*
 * Model classes and objects used during query parsing
 */

object QueryOperation extends Enumeration {
  val Scan = Value
}

case class QueryVersions(numVersions : Int)

object QueryVersions {
  val All = new QueryVersions(Int.MaxValue)
  val One = new QueryVersions(1)
}

sealed abstract class RowConstraint
case class SingleRowConstraint(operator : String, parameter : String) extends RowConstraint
case class BetweenRowConstraint(start : String, stop : String) extends RowConstraint

sealed abstract class Qualifier
case class EmptyQualifier() extends Qualifier
case class EmptyPrefixQualifier() extends Qualifier
case class PrefixQualifier(qualifier : String) extends Qualifier
case class StandardQualifier(qualifier : String) extends Qualifier

case class Column(family    : String                   = "",
                  qualifier : Qualifier                = EmptyQualifier(),
                  versions  : QueryVersions            = QueryVersions.One,
                  timeRange : Option[(String, String)] = None)
