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
