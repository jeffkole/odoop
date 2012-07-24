package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.util.Bytes

import org.scalatest.FunSpec

import com.opower.hadoop.hbase.filter.ColumnVersionTimerangeFilter
import com.opower.hadoop.hbase.filter.FamilyOnlyColumnVersionTimerangeFilter
import com.opower.hadoop.hbase.filter.QualifierPrefixColumnVersionTimerangeFilter

import scala.collection.JavaConverters._

/**
 * Behavioral tests that can be shared across QueryBuilderSpec tests
 *
 * @author jeff@opower.com
 */
trait QueryBuilderScanBehaviors { this: FunSpec =>
  def defaultScan(scan : Scan) = {
    assert(scan.getFamilies === null)
    assert(scan.getFamilyMap.isEmpty)
    assert(scan.getFilter === null)
    assert(scan.getMaxVersions === 1)
    assert(scan.getStartRow.size === 0)
    assert(scan.getStopRow.size === 0)
    assert(scan.getTimeRange.getMin === 0L)
    assert(scan.getTimeRange.getMax === Long.MaxValue)
  }

  def scanWithFamilySet(scan : Scan, family : Array[Byte]) = {
    assert(scan.numFamilies === 1)
    assert(scan.getFamilyMap.containsKey(family))
  }

  def scanWithFamiliesSet(scan : Scan, families : List[Array[Byte]]) = {
    assert(scan.numFamilies === families.size)
    for (family <- families) {
      assert(scan.getFamilyMap.containsKey(family))
    }
  }

  def scanWithQualifiersSet(scan : Scan, qualifierMap : Map[String, List[String]]) = {
    val familyMap = scan.getFamilyMap
    for ((family, qualifiers) <- qualifierMap) {
      val f = Bytes.toBytesBinary(family)
      if (qualifiers.isEmpty) {
        assert(familyMap.get(f) === null)
      }
      else {
        val actualQualifiers = familyMap.get(f).asScala.map(Bytes.toStringBinary(_))
          assert(actualQualifiers.size === qualifiers.size)
        for (qualifier <- qualifiers) {
          assert(actualQualifiers.contains(qualifier))
        }
      }
    }
  }

  def scanWithFilterList(scan : Scan, operator : FilterList.Operator, filters : List[Filter]) = {
    assert(scan.hasFilter)
    assert(scan.getFilter.isInstanceOf[FilterList])
    val filterList = scan.getFilter.asInstanceOf[FilterList]
    assert(filterList.getOperator === operator)
    val filterListFilters = filterList.getFilters
    assert(filterListFilters.size === filters.size)
    for (i <- 0 until filters.size) {
      val flf = filterListFilters.get(i)
      val f = filters(i)
      assert(flf.getClass === f.getClass)
      f match {
        case c : ColumnVersionTimerangeFilter => {
          assertFilter(flf.asInstanceOf[ColumnVersionTimerangeFilter],
            f.asInstanceOf[ColumnVersionTimerangeFilter])
        }
        case c : QualifierPrefixColumnVersionTimerangeFilter => {
          assertFilter(flf.asInstanceOf[QualifierPrefixColumnVersionTimerangeFilter],
            f.asInstanceOf[QualifierPrefixColumnVersionTimerangeFilter])
        }
        case c : FamilyOnlyColumnVersionTimerangeFilter => {
          assertFilter(flf.asInstanceOf[FamilyOnlyColumnVersionTimerangeFilter],
            f.asInstanceOf[FamilyOnlyColumnVersionTimerangeFilter])
        }
        case c : Filter => fail("oh shit " + c)
      }
    }
  }

  def assertFilter(actual : ColumnVersionTimerangeFilter, expected : ColumnVersionTimerangeFilter) = {
    assert(actual.getFamily === expected.getFamily)
    assert(actual.getQualifier === expected.getQualifier)
    assert(actual.getMaxVersions === expected.getMaxVersions)
    assert(actual.getStartTimestamp === expected.getStartTimestamp)
    assert(actual.getStopTimestamp === expected.getStopTimestamp)
  }

  def assertFilter(actual : FamilyOnlyColumnVersionTimerangeFilter, expected : FamilyOnlyColumnVersionTimerangeFilter) = {
    assert(actual.getFamily === expected.getFamily)
    assert(actual.getMaxVersions === expected.getMaxVersions)
    assert(actual.getStartTimestamp === expected.getStartTimestamp)
    assert(actual.getStopTimestamp === expected.getStopTimestamp)
  }

  def assertFilter(actual : QualifierPrefixColumnVersionTimerangeFilter,
    expected : QualifierPrefixColumnVersionTimerangeFilter) = {
    assert(actual.getFamily === expected.getFamily)
    assert(actual.getQualifierPrefix === expected.getQualifierPrefix)
    assert(actual.getMaxVersions === expected.getMaxVersions)
    assert(actual.getStartTimestamp === expected.getStartTimestamp)
    assert(actual.getStopTimestamp === expected.getStopTimestamp)
  }
}
