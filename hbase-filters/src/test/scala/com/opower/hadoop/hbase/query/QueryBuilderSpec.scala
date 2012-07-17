package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.InclusiveStopFilter
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.util.Bytes

import org.junit.runner.RunWith

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class QueryBuilderSpec extends FunSpec with BeforeAndAfter with GivenWhenThen with ShouldMatchers {
  implicit def string2BinaryByteArray(string : String) : Array[Byte] = {
    Bytes.toBytesBinary(string)
  }

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

    it("should leave a scan's defaults for a simple query") {
      given("just a scan set")
      builder.scan

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have all defaults set")
      scan.getFamilies should be (null)
    }

    it("should add all columns in the query to the scan") {
      given("a builder with two columns")
      builder.addColumnDefinition(Column("family", "one"))
      builder.addColumnDefinition(Column("family", "two"))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have columns added")
      val familyMap = scan.getFamilyMap
      familyMap should have size (1)
      val qualifiers = familyMap.get(Bytes.toBytesBinary("family"))
      qualifiers should have size (2)
      // comparison of byte arrays does not work well, so convert all to strings first
      val stringQualifiers = qualifiers.asScala.map(Bytes.toStringBinary(_))
      stringQualifiers should contain ("one")
      stringQualifiers should contain ("two")
    }

    // the scan time range must not be set as well
    it("should add timestamp filters for columns that have timestamps specified") (pending)

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

    // the scan versions must be set to max versions
    it("should add version filters for columns that have versions specified") (pending)

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
