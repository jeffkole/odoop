package com.opower.hadoop.hbase.query

import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit

import org.junit.Before
import org.junit.Test

class TestQueryParser extends JUnitSuite with ShouldMatchersForJUnit {
  var parser : QueryParser = null

  @Before
  def setUp() {
    this.parser = new QueryParser(null)
  }

  @Test
  def testPositiveWholeNumberMatchesCorrectly() {
    runSuccessfulParse[String](parser, parser.positiveWholeNumber, "19", "19")
  }

  @Test
  def testPositiveWholeNumberDoesNotMatchNegative() {
    runFailedParse[String](parser, parser.positiveWholeNumber, "-19")
  }

  @Test
  def testPositiveWholeNumberDoesNotMatchLetters() {
    runFailedParse[String](parser, parser.positiveWholeNumber, "abc")
  }

  @Test
  def testPositiveWholeNumberDoesNotMatchNumberInMiddle() {
    runFailedParse[String](parser, parser.positiveWholeNumber, "a19b")
  }

  @Test
  def testPositiveWholeNumberDoesNotMatchNumberAsToken() {
    runFailedParse[String](parser, parser.positiveWholeNumber, "a 19 b")
  }

  @Test
  def testPositiveWholeNumberDoesNotMatchDecimalNumber() {
    runFailedParse[String](parser, parser.positiveWholeNumber, "10.42")
  }
  // Cannot have a path-dependent type of `parser.Parser[T]` in the parameter type definition, but we need that
  // type to match the parameter types of `parser.parseAll`, so the cast is required.
  private def runSuccessfulParse[T](parser : QueryParser, term : QueryParser#Parser[T], input : String, expected : T) {
    parser.parseAll(term.asInstanceOf[parser.Parser[T]], input) match {
      case parser.Success(result, next) => {
        result should equal (expected)
        next.atEnd should be (true)
      }
      case parser.Failure(msg, next) => fail(msg)
    }
  }

  private def runFailedParse[T](parser : QueryParser, term : QueryParser#Parser[T], input : String) {
    parser.parseAll(term.asInstanceOf[parser.Parser[T]], input) match {
      case parser.Success(result, next) => fail("Should not have matched " + result)
      case parser.Failure(msg, next) => { /* success! */ }
    }
  }
}
