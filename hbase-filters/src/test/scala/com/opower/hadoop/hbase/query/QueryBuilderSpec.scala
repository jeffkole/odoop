package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes

import org.junit.runner.RunWith

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.GivenWhenThen
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

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
      given("a scan with two columns")
      builder.scan
      builder.addColumnDefinition(Column("family", "one"))
      builder.addColumnDefinition(Column("family", "two"))

      when("a scan is planned")
      val scan = builder.doPlanScan(noParameters, noTimestamps)

      then("the scan should have columns added")
      val familyMap = scan.getFamilyMap
      familyMap should have size (1)
      val qualifiers = familyMap.get("family")
      qualifiers should have size (2)
      qualifiers should contain (string2BinaryByteArray("one")) // unclear to me why the implicit isn't picked up here
      qualifiers should contain (string2BinaryByteArray("two"))
    }

    it("should add timestamp filters for columns that have timestamps specified") (pending)

    it("should add version filters for columns that have versions specified") (pending)

  }
}
