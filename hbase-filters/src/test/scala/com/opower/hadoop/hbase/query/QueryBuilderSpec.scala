package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan
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
      builder.getTableName should be ('empty)

      when("a table name is set")
      builder.setTableName("tableName")

      then("the table name should be stored")
      builder.getTableName should not be ('empty)
      builder.getTableName.get should equal ("tableName")
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

    // the scan versions must be set to max versions
    it("should add version filters for columns that have versions specified") (pending)

    it("should add a start row for a >= rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)

      given("a builder with a >= rowkey constraint")
      builder.addConstraint(RowConstraint(">=", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a start row set")
      scan.getStartRow should equal (idValue)
    }

    it("should add a stop row for a < rowkey constraint") {
      val id = "id"
      val idValue = Array[Byte](0xF)

      given("a builder with a < rowkey constraint")
      builder.addConstraint(RowConstraint("<", id))

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
      builder.addConstraint(RowConstraint("=", id))

      when("a scan is planned")
      val scan = builder.doPlanScan(Map(id -> idValue), noTimestamps)

      then("the scan should have a start and stop row set")
      scan.getStartRow should equal (idValue)
      scan.getStopRow should equal (stopValue)
    }

    it("should add a start row and a rowkey filter for a > rowkey constraint") (pending)

    it("should add a stop row and a rowkey filter for a <= rowkey constraint") (pending)
  }
}