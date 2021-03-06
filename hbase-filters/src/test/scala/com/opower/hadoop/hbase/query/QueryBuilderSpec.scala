package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.InclusiveStopFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.util.Bytes

import org.junit.runner.RunWith

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import com.opower.hadoop.hbase.filter.ColumnVersionTimerangeFilter
import com.opower.hadoop.hbase.filter.FamilyOnlyColumnVersionTimerangeFilter
import com.opower.hadoop.hbase.filter.QualifierPrefixColumnVersionTimerangeFilter

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class QueryBuilderSpec extends FunSpec
  with BeforeAndAfter with GivenWhenThen with ShouldMatchers with QueryBuilderScanBehaviors {

  implicit def string2BinaryByteArray(string : String) : Array[Byte] = Bytes.toBytesBinary(string)
  implicit def string2Qualifier(string : String) : Qualifier = StandardQualifier(string)

  val noParameters = Map.empty[String, Array[Byte]]
  val noTimestamps = Map.empty[String, Long]

  var builder : QueryBuilder = _

  before {
    builder = new QueryBuilder("")
  }

  describe("Population of a QueryBuilder") {

    it("should store a table name") {
      given("no table name")
      builder.getTableName should be (null)

      when("a table name is set")
      builder.setTableName("tableName")

      then("the table name should be stored")
      builder.getTableName should equal ("tableName")
    }

  }

  describe("Planning a scan") {

    it("should leave the scan defaults for a simple query") {
      given("just a scan set")
      builder.scan

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have all defaults set")
      it should behave like defaultScan(scan)
    }

    it("should add all columns in the query to the scan") {
      given("a builder with two columns")
      builder.addColumnDefinition(Column("family", "one"))
      builder.addColumnDefinition(Column("family", "two"))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have columns added")
      it should behave like scanWithFamilySet(scan, "family")
      it should behave like scanWithQualifiersSet(scan, Map("family" -> List("one", "two")))
    }

    it("should add timestamp filters for columns that have timestamps specified") {
      given("a builder with columns with timeranges")
      builder.addColumnDefinition(Column("family", "one", QueryVersions.One, Some(("start", "stop"))))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, Map("start" -> 100L, "stop" -> 500L))

      then("the scan should have a filter with the timestamp set")
      scan.getTimeRange.getMin should equal (100L)
      scan.getTimeRange.getMax should equal (500L)
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new ColumnVersionTimerangeFilter("family", "one", 1, 100L, 500L)))
    }

    it("should set family for columns with empty prefixes") {
      given("a builder with columns with empty prefixes")
      builder.addColumnDefinition(Column("family", EmptyPrefixQualifier()))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should set the family")
      it should behave like scanWithFamilySet(scan, "family")
    }

    it("should set a family prefix filter for columns with empty prefixes") {
      given("a builder with columns with empty prefixes")
      builder.addColumnDefinition(Column("f1", "one", QueryVersions(3)))
      builder.addColumnDefinition(Column("f2", EmptyPrefixQualifier()))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new FamilyOnlyColumnVersionTimerangeFilter("f2", 1),
          new ColumnVersionTimerangeFilter("f1", "one", 3)))
    }

    it("should set family and filter for columns with qualifier prefixes") {
      given("a builder with columns with quailfier prefixes")
      builder.addColumnDefinition(Column("family", PrefixQualifier("qual")))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should set the family")
      it should behave like scanWithFamilySet(scan, "family")
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new QualifierPrefixColumnVersionTimerangeFilter("family", "qual", 1)))
    }

    it("should set a qualifier prefix filter for columns with qualifier prefixes") {
      given("a builder with columns with qualifier prefixes")
      builder.addColumnDefinition(Column("f1", "one", QueryVersions(3)))
      builder.addColumnDefinition(Column("f1", PrefixQualifier("qual")))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new QualifierPrefixColumnVersionTimerangeFilter("f1", "qual", 1),
          new ColumnVersionTimerangeFilter("f1", "one", 3)))
    }

    it("should set a qualifier prefix filter for columns with qualifier prefixes even with the same number of versions") {
      given("a builder with columns with qualifier prefixes")
      builder.addColumnDefinition(Column("f1", "one"))
      builder.addColumnDefinition(Column("f1", PrefixQualifier("qual")))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new QualifierPrefixColumnVersionTimerangeFilter("f1", "qual", 1),
          new ColumnVersionTimerangeFilter("f1", "one", 1)))
    }

    it("should set only the family if there are qualifier prefix columns in the same family") {
      given("a builder with qualifier prefix column in family A and regular column in family A")
      builder.addColumnDefinition(Column("A", "one"))
      builder.addColumnDefinition(Column("A", PrefixQualifier("pre")))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      it should behave like scanWithFamilySet(scan, "A")
      it should behave like scanWithQualifiersSet(scan, Map("A" -> Nil))
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new QualifierPrefixColumnVersionTimerangeFilter("A", "pre", 1),
          new ColumnVersionTimerangeFilter("A", "one", 1)))
    }

    it("should set family and qualifier if there are qualifier prefix columns in different families") {
      given("a builder with qualifier prefix column in family A and regular column in family B")
      builder.addColumnDefinition(Column("B", "one"))
      builder.addColumnDefinition(Column("A", PrefixQualifier("pre")))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      it should behave like scanWithFamiliesSet(scan, List("A", "B"))
      it should behave like scanWithQualifiersSet(scan, Map("A" -> Nil, "B" -> List("one")))
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new QualifierPrefixColumnVersionTimerangeFilter("A", "pre", 1),
          new ColumnVersionTimerangeFilter("B", "one", 1)))
    }

    it("should set the timerange on the scan if all columns have timeranges set") {
      given("a builder with multiple columns with timeranges set")
      builder.addColumnDefinition(Column("family", "one", QueryVersions.One, Some(("start1", "stop1"))))
      builder.addColumnDefinition(Column("family", "two", QueryVersions.One, Some(("start2", "stop2"))))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters,
        Map("start1" -> 100L, "stop1" -> 500L,
            "start2" -> 300L, "stop2" -> 800L))

      then("the scan should have a timerange that covers all column timeranges")
      scan.getTimeRange.getMin should equal (100L)
      scan.getTimeRange.getMax should equal (800L)

      // the builder operates in reverse, so the columns are defined backwards
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new ColumnVersionTimerangeFilter("family", "two", 1, 300L, 800L),
          new ColumnVersionTimerangeFilter("family", "one", 1, 100L, 500L)))
    }

    it("should not set the timerange on the scan if there is a mix of timeranges on columns") {
      given("a builder with some columns with timestamps and some without")
      builder.addColumnDefinition(Column("family", "one"))
      builder.addColumnDefinition(Column("family", "two", QueryVersions.One, Some(("start", "stop"))))
      builder.addColumnDefinition(Column("family", "three"))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, Map("start" -> 100L, "stop" -> 500L))

      then("the scan should have no timerange set")
      // Scan sets a default TimeRange for 'allTime', so that needs to be checked explicitly
      scan.getTimeRange.getMin should equal (0L)
      scan.getTimeRange.getMax should equal (Long.MaxValue)
    }

    it("should set filters for all columns if one of them uses versions or timestamps") {
      given("a builder with a column with all versions and a timerange and a column with all versions only")
      builder.addColumnDefinition(Column("family", "one", QueryVersions.All, Some(("start", "stop"))))
      builder.addColumnDefinition(Column("family", "two", QueryVersions.All))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, Map("start" -> 100L, "stop" -> 500L))

      then("the scan should have a no timerange set and should have a filter for both")
      scan.getMaxVersions should equal(Int.MaxValue)
      scan.getTimeRange.getMin should equal (0L)
      scan.getTimeRange.getMax should equal (Long.MaxValue)

      // the builder operates in reverse, so the columns are defined backwards
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(new ColumnVersionTimerangeFilter("family", "two", Int.MaxValue, Long.MinValue, Long.MaxValue),
          new ColumnVersionTimerangeFilter("family", "one", Int.MaxValue, 100L, 500L)))
    }

    // the scan versions must be set to max versions
    it("should add version filters for columns that have versions specified") {
      given("a builder with multiple columns with versions set")
      builder.addColumnDefinition(Column("family", "one", QueryVersions(5)))
      builder.addColumnDefinition(Column("family", "two", QueryVersions.One))
      builder.addColumnDefinition(Column("family", "three", QueryVersions.All))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have version filters set for the columns")
      scan.getMaxVersions should equal(Int.MaxValue)
      scan.getTimeRange.getMin should equal (0)
      scan.getTimeRange.getMax should equal (Long.MaxValue)

      // the builder operates in reverse, so the columns are defined backwards
      it should behave like scanWithFilterList(scan, FilterList.Operator.MUST_PASS_ONE,
        List(
          new ColumnVersionTimerangeFilter("family", "three", Int.MaxValue, Long.MinValue, Long.MaxValue),
          new ColumnVersionTimerangeFilter("family", "two", 1, Long.MinValue, Long.MaxValue),
          new ColumnVersionTimerangeFilter("family", "one", 5, Long.MinValue, Long.MaxValue)))
    }

    it("should not set version filters on columns if all of the column versions are the same") {
      given("a builder with multiple columns with the same versions set")
      builder.addColumnDefinition(Column("family", "one", QueryVersions(5)))
      builder.addColumnDefinition(Column("family", "two", QueryVersions(5)))
      builder.addColumnDefinition(Column("family", "three", QueryVersions(5)))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have no version filters set for the columns")
      scan.getMaxVersions should equal(5)
      scan.getTimeRange.getMin should equal (0)
      scan.getTimeRange.getMax should equal (Long.MaxValue)
      scan.hasFilter should be (false)
    }

    it("should set the max versions on the scan to the max across all columns") {
      given("a builder with columns with versions set")
      builder.addColumnDefinition(Column("family", "one")) // default versions is 1
      builder.addColumnDefinition(Column("family", "two", QueryVersions(2)))
      builder.addColumnDefinition(Column("family", "three", QueryVersions(3)))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have max versions set")
      scan.getMaxVersions should equal(3)
    }

    it("should set the max versions on the scan to the max across all columns including 'all'") {
      given("a builder with columns with versions set")
      builder.addColumnDefinition(Column("family", "one")) // default versions is 1
      builder.addColumnDefinition(Column("family", "all", QueryVersions.All))
      builder.addColumnDefinition(Column("family", "three", QueryVersions(3)))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have max versions set")
      scan.getMaxVersions should equal(QueryVersions.All.numVersions)
    }

    it("should leave max versions alone with no columns defined") {
      given("a builder with no columns set")

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have the default versions set")
      scan.getMaxVersions should equal(1)
    }

    it("should add a start row for a >= rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)

      given("a builder with a >= rowkey constraint")
      builder.addConstraint(SingleRowConstraint(">=", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a start row set")
      scan.getStartRow should equal (idValue)
    }

    it("should add a stop row for a < rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)

      given("a builder with a < rowkey constraint")
      builder.addConstraint(SingleRowConstraint("<", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a stop row set")
      scan.getStopRow should equal (idValue)
    }

    it("should add a start and stop row for a = rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)
      val stopValue = Array[Byte](0xF, 0x0)

      given("a builder with a = rowkey constraint")
      builder.addConstraint(SingleRowConstraint("=", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a start and stop row set")
      scan.getStartRow should equal (idValue)
      scan.getStopRow should equal (stopValue)
    }

    it("should add a start row and a rowkey filter for a > rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)

      given("a builder with a > rowkey constraint")
      builder.addConstraint(SingleRowConstraint(">", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a start row and a rowkey filter set")
      scan.getStartRow should equal (idValue)
      scan.hasFilter should be (true)
      scan.getFilter.isInstanceOf[RowFilter] should be (true)
      val filter = scan.getFilter.asInstanceOf[RowFilter]
      filter.getOperator should equal (CompareOp.GREATER)
      filter.getComparator.getValue should equal (idValue)
    }

    it("should add a rowkey filter for a <= rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)

      given("a builder with a <= rowkey constraint")
      builder.addConstraint(SingleRowConstraint("<=", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a stop row and a rowkey filter set")
      scan.getStopRow should have length (0)
      scan.hasFilter should be (true)
      scan.getFilter.isInstanceOf[InclusiveStopFilter] should be (true)
      val filter = scan.getFilter.asInstanceOf[InclusiveStopFilter]
      filter.getStopRowKey should equal (idValue)
    }

    it("should add a start and stop key for a between rowkey constraint") {
      val lowParam = "low"
      val highParam = "high"
      val lowVal = Array[Byte](0xA, 0xB, 0xC)
      val highVal = Array[Byte](0xF, 0xD, 0xE)

      given("a builder with a between rowkey constraint")
      builder.addConstraint(BetweenRowConstraint("low", "high"))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(lowParam -> lowVal, highParam -> highVal), noTimestamps)

      then("the scan should have a start row and stop row set")
      scan.getStartRow should equal (lowVal)
      scan.getStopRow should equal (highVal)
    }
  }

  describe("A failed planned scan") {

    it("should throw an exception if rowkey constraint parameters are not set") {
      given("a builder with a rowkey constraint")
      builder.addConstraint(SingleRowConstraint("=", "id"))

      when("a scan is planned")
      val exception = evaluating { builder.doPlanScan(noParameters, noTimestamps) } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing parameter")
    }

    it("should throw an exception if neither rowkey between parameter is set") {
      given("a builder with a rowkey between constraint")
      builder.addConstraint(BetweenRowConstraint("low", "high"))

      when("a scan is planned")
      val exception = evaluating { builder.doPlanScan(noParameters, noTimestamps) } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing parameter")
    }

    it("should throw an exception if the first rowkey between parameter is not set") {
      given("a builder with a rowkey between constraint")
      builder.addConstraint(BetweenRowConstraint("low", "high"))

      when("a scan is planned")
      val exception = evaluating {
        builder.doPlanScan(Map("high" -> Array[Byte]()), noTimestamps)
      } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing parameter")
    }

    it("should throw an exception if the second rowkey between parameter is not set") {
      given("a builder with a rowkey between constraint")
      builder.addConstraint(BetweenRowConstraint("low", "high"))

      when("a scan is planned")
      val exception = evaluating {
        builder.doPlanScan(Map("low" -> Array[Byte]()), noTimestamps)
      } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing parameter")
    }

    it("should throw an exception if the column timestamps are not set") {
      given("a builder with a column with a timerange")
      builder.addColumnDefinition(Column("family", "qualifier", QueryVersions.One, Some(("start", "stop"))))

      when("a scan is planned")
      val exception = evaluating { builder.doPlanScan(noParameters, noTimestamps) } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing timestamp parameter")
    }

    it("should throw an exception if the first column timestamp is not set") {
      given("a builder with a column with a timerange")
      builder.addColumnDefinition(Column("family", "qualifier", QueryVersions.One, Some(("start", "stop"))))

      when("a scan is planned")
      val exception = evaluating {
        builder.doPlanScan(noParameters, Map("stop" -> 100L))
      } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing timestamp parameter")
    }

    it("should throw an exception if the second column timestamp is not set") {
      given("a builder with a column with a timerange")
      builder.addColumnDefinition(Column("family", "qualifier", QueryVersions.One, Some(("start", "stop"))))

      when("a scan is planned")
      val exception = evaluating {
        builder.doPlanScan(noParameters, Map("start" -> 10L))
      } should produce [IllegalArgumentException]

      then("an exception should have been thrown")
      exception should not be (null)
      exception.getMessage should startWith ("Missing timestamp parameter")
    }
  }
}
