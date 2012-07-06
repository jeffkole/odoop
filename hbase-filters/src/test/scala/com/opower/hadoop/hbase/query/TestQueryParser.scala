package com.opower.hadoop.hbase.query

import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit

import org.junit.Test

class TestQueryParser extends JUnitSuite with ShouldMatchersForJUnit {
  @Test
  def testPositiveWholeNumberMatchesCorrectly() {
    val parser = new QueryParser(null)
    val input = "19"
    val parseResult = parser.parseAll(parser.positiveWholeNumber, input)
    parseResult match {
      case parser.Success(result, next) => {
        result should equal ("19")
        next.atEnd should be (true)
      }
      case parser.Failure(msg, next) => fail(msg)
    }
  }

  @Test
  def testPositiveWholeNumberDoesNotMatchNegative() {
    val parser = new QueryParser(null)
    val input = "-19"
    val parseResult = parser.parseAll(parser.positiveWholeNumber, input)
    parseResult match {
      case parser.Success(result, next) => fail("Should not have found " + result)
    }
  }
}
