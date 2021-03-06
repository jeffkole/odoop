package com.opower.hadoop.hbase.query

import org.apache.hadoop.hbase.util.Bytes

import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit

import org.junit.Before
import org.junit.Test

class TestQueryParser extends JUnitSuite with ShouldMatchersForJUnit {
  implicit def string2BinaryByteArray(string : String) : Array[Byte] = {
    Bytes.toBytesBinary(string)
  }

  implicit def string2Qualifier(string : String) : Qualifier = StandardQualifier(string)

  var builder : QueryBuilder = null
  var parser : QueryParser = null

  @Before
  def setUp() {
    this.builder = new QueryBuilder("")
    this.parser = new QueryParser(this.builder)
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

  @Test
  def testParameterMatchesCorrectly() {
    runSuccessfulParse[String](parser, parser.parameter, "{paramName}", "paramName")
  }

  @Test
  def testParameterWithWhitespaceWorks() {
    runSuccessfulParse[String](parser, parser.parameter, "{ paramName }", "paramName")
  }

  @Test
  def testParameterMissingBrackets() {
    runFailedParse[String](parser, parser.parameter, "paramName")
  }

  @Test
  def testRowKeyOperatorMatchesCorrectly() {
    for (op <- Array("<", "<=", ">", ">=", "=")) {
      runSuccessfulParse[String](parser, parser.rowKeyOperator, op, op)
    }
  }

  @Test
  def testRowKeyOperatorMismatches() {
    for (op <- Array("!", "&&", "||", "<>")) {
      runFailedParse[String](parser, parser.rowKeyOperator, op)
    }
  }

  @Test
  def testRowKeyConstraintMatches() {
    runSuccessfulParse[RowConstraint](parser, parser.rowKeyConstraint, "rowkey = {id}", SingleRowConstraint("=", "id"))
  }

  @Test
  def testRowKeyConstraintWhitespace() {
    runSuccessfulParse[RowConstraint](parser, parser.rowKeyConstraint, "rowkey<={ id }", SingleRowConstraint("<=", "id"))
  }

  @Test
  def testRowKeyPartialMatchFails() {
    runFailedParse[RowConstraint](parser, parser.rowKeyConstraint, "rowkey == {id}")
  }

  @Test
  def testRowKeyBetweenMatches() {
    runSuccessfulParse[RowConstraint](parser, parser.rowKeyConstraint, "rowkey between {X} and {Y}",
      BetweenRowConstraint("X", "Y"))
  }

  @Test
  def testWhereClauseMatches() {
    val expectedConstraint = SingleRowConstraint("=", "id")
    this.builder.getRowConstraints should be ('empty)
    runSuccessfulParse[RowConstraint](parser, parser.whereClause, "where rowkey = {id}", expectedConstraint)
    this.builder.getRowConstraints should not be ('empty)
    this.builder.getRowConstraints.head should equal (expectedConstraint)
  }

  @Test
  def testWhereClauseMatchesBetween() {
    val expectedConstraint = BetweenRowConstraint("low", "high")
    this.builder.getRowConstraints should be ('empty)
    runSuccessfulParse[RowConstraint](parser, parser.whereClause, "where rowkey between {low} and {high}", expectedConstraint)
    this.builder.getRowConstraints should not be ('empty)
    this.builder.getRowConstraints.head should equal (expectedConstraint)
  }

  @Test
  def testTableNameMatchesValidTableNames() {
    for (tableName <- Array("t", "table_name", "aTable", "table-one", "table.name", "987654321")) {
      runSuccessfulParse[String](parser, parser.tableName, tableName, tableName)
    }
  }

  @Test
  def testTableNameSkipsInvalidTableNames() {
    for (tableName <- Array(".META.", "-ROOT-", "", "this is a table")) {
      runFailedParse[String](parser, parser.tableName, tableName)
    }
  }

  @Test
  def testFromClauseMatches() {
    val expectedTableName = "tableName"
    this.builder.getTableName should be (null)
    runSuccessfulParse[String](parser, parser.fromClause, "from tableName", expectedTableName)
    this.builder.getTableName should equal (expectedTableName)
  }

  @Test
  def testTimeRangeMatches() {
    runSuccessfulParse[(String, String)](parser, parser.timeRange, "between {start} and {stop}", ("start", "stop"))
  }

  @Test
  def testTimeRangeSkipsInvalidRange() {
    runFailedParse[(String, String)](parser, parser.timeRange, "between {start} & {stop}")
  }

  @Test
  def testColumnQualifierMatches() {
    runSuccessfulParse[Qualifier](parser, parser.columnQualifier, "bigData", StandardQualifier("bigData"))
  }

  @Test
  def testColumnQualifierMatchesCrazyBytes() {
    val qualifier = Bytes.toStringBinary(Array[Byte]('\100', '\24', 0xF, 0, 0x8))
    runSuccessfulParse[Qualifier](parser, parser.columnQualifier, qualifier, StandardQualifier(qualifier))
  }

  @Test
  def testColumnQualifierMatchesCrazyCharacters() {
    val qualifier = """abcdeABDCE01234`~!@#$%^&()-_=+[]{}\|;:'".<>/?"""
    runSuccessfulParse[Qualifier](parser, parser.columnQualifier, qualifier, StandardQualifier(qualifier))
  }

  @Test
  def testColumnQualifierStopsOnComma() {
    val qualifier = """abcdeABDCE01234,.<>/?"""
    runFailedParse[Qualifier](parser, parser.columnQualifier, qualifier)
  }

  @Test
  def testColumnQualifierStopsOnSpace() {
    val qualifier = """abcdeABDCE01234 .<>/?"""
    runFailedParse[Qualifier](parser, parser.columnQualifier, qualifier)
  }

  @Test
  def testColumnQualifierSkipsNonBinaryFormattedBytes() {
    val qualifier = Bytes.toString(Array[Byte]('\100', '\24', 0xF, 0, 0x8))
    runFailedParse[Qualifier](parser, parser.columnQualifier, qualifier)
  }

  @Test
  def testColumnQualifierWildcardMatches() {
    runSuccessfulParse[Qualifier](parser, parser.columnQualifier, "*", EmptyPrefixQualifier())
  }

  @Test
  def testColumnQualifierPrefixWildcardMatches() {
    runSuccessfulParse[Qualifier](parser, parser.columnQualifier, "qualifier*", PrefixQualifier("qualifier"))
  }

  @Test
  def testWildcardAtTheStartOfColumnQualifierFails() {
    runFailedParse[Qualifier](parser, parser.columnQualifier, "*qualifier")
  }

  @Test
  def testWildcardInTheMiddleOfColumnQualifierFails() {
    runFailedParse[Qualifier](parser, parser.columnQualifier, "qualif*ier")
  }

  @Test
  def testColumnFamilyMatches() {
    runSuccessfulParse[String](parser, parser.columnFamily, "family", "family")
  }

  @Test
  def testColumnFamilySkipsBytes() {
    val qualifier = Bytes.toStringBinary(Array[Byte]('\100', '\24', 0xF, 0, 0x8))
    runFailedParse[String](parser, parser.columnFamily, qualifier)
  }

  @Test
  def testSomeVersionsMatches() {
    runSuccessfulParse[QueryVersions](parser, parser.someVersions, "12 versions of", QueryVersions(12))
  }

  @Test
  def testSomeVersionsMatchesSingleVersion() {
    runSuccessfulParse[QueryVersions](parser, parser.someVersions, "1 version of", QueryVersions(1))
  }

  @Test
  def testSomeVersionsSkipsWordNumbers() {
    runFailedParse[QueryVersions](parser, parser.someVersions, "no version of")
  }

  @Test
  def testAllVersionsMatches() {
    runSuccessfulParse[QueryVersions](parser, parser.allVersions, "all versions of", QueryVersions.All)
  }

  @Test
  def testAllVersionsSkipsBadInput() {
    runFailedParse[QueryVersions](parser, parser.allVersions, "some versions of")
  }

  @Test
  def testColumnDefinitionMatches() {
    this.builder.getColumns should be ('empty)
    val family = "family"
    val qualifier = "qualifier"
    val expectedColumn = Column(family, qualifier, QueryVersions.One, None)
    runSuccessfulParse[Column](parser, parser.columnDefinition, family + ":" + qualifier, expectedColumn)
    this.builder.getColumns should not be ('empty)
    this.builder.getColumns.head should equal (expectedColumn)
  }

  @Test
  def testScanClauseMatches() {
    val scanClause = "scan all versions of d:one"
    val expectedColumn = Column("d", "one", QueryVersions.All)
    this.builder.getQueryOperation should be ('empty)
    this.builder.getColumns should be ('empty)
    runSuccessfulParse[List[Column]](parser, parser.scanClause, scanClause, List(expectedColumn))
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getColumns should not be ('empty)
    this.builder.getColumns.head should equal (expectedColumn)
  }

  @Test
  def testScanClauseMatchesAllColumns() {
    val scanClause = "scan"
    this.builder.getQueryOperation should be ('empty)
    this.builder.getColumns should be ('empty)
    runSuccessfulParse[List[Column]](parser, parser.scanClause, scanClause, List())
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getColumns should be ('empty)
  }

  @Test
  def testScanClauseMatchesMultipleColumns() {
    val scanClause = "scan all versions of d:one, d:two, 3 versions of d:three"
    val family = "d"
    val expectedColumns = List(
      Column(family, "one", QueryVersions.All),
      Column(family, "two"),
      Column(family, "three", QueryVersions(3)))
    this.builder.getQueryOperation should be ('empty)
    this.builder.getColumns should be ('empty)
    runSuccessfulParse[List[Column]](parser, parser.scanClause, scanClause, expectedColumns)
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getColumns should not be ('empty)
    // the columns are built up using `::`, so they will be reversed
    this.builder.getColumns should equal (expectedColumns.reverse)
  }

  @Test
  def testScanClauseMatchesMultipleColumnsWithTimeRanges() {
    val scanClause = "scan all versions of d:one, d:two between {start} and {stop}, 3 versions of d:three"
    val family = "d"
    val expectedColumns = List(
      Column(family, "one", QueryVersions.All),
      Column(family, "two", QueryVersions.One, Some(("start", "stop"))),
      Column(family, "three", QueryVersions(3)))
    this.builder.getQueryOperation should be ('empty)
    this.builder.getColumns should be ('empty)
    runSuccessfulParse[List[Column]](parser, parser.scanClause, scanClause, expectedColumns)
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getColumns should not be ('empty)
    // the columns are built up using `::`, so they will be reversed
    this.builder.getColumns should equal (expectedColumns.reverse)
  }

  @Test
  def testQueryMatches() {
    val query = """scan d:one, 1 version of d:two, 2 versions of d:three between {start} and {stop},
      3 versions of d:four, all versions of d:five between {start} and {stop} from table where rowkey <= {maxId}"""
    val tableName = "table"
    val family = "d"
    val expectedColumns = List(
      Column(family, "one"),
      Column(family, "two", QueryVersions.One),
      Column(family, "three", QueryVersions(2), Some(("start", "stop"))),
      Column(family, "four", QueryVersions(3)),
      Column(family, "five", QueryVersions.All, Some(("start", "stop"))))
    val expectedRowConstraint = SingleRowConstraint("<=", "maxId")
    // "stable identifier required", so make a new val to use
    val parserVal = parser
    val expectedResult = new parserVal.~(new parserVal.~(expectedColumns, tableName), Some(expectedRowConstraint))

    this.builder.getQueryOperation should be ('empty)
    this.builder.getTableName should be (null)
    this.builder.getColumns should be ('empty)
    this.builder.getRowConstraints should be ('empty)
    runSuccessfulParse[Any](parser, parser.query, query, expectedResult)
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getTableName should equal (tableName)
    this.builder.getColumns should not be ('empty)
    this.builder.getColumns should equal (expectedColumns.reverse)
    this.builder.getRowConstraints should not be ('empty)
    this.builder.getRowConstraints.head should equal (expectedRowConstraint)
  }

  @Test
  def testQueryWithWildcardsMatches() {
    val query = """scan d:one, 1 version of d:two, 2 versions of d:three between {start} and {stop},
      3 versions of d:four*, all versions of d:* between {start} and {stop}, e:*, f:c* from table where rowkey = {id}"""
    val tableName = "table"
    val expectedColumns = List(
      Column("d", "one"),
      Column("d", "two", QueryVersions.One),
      Column("d", "three", QueryVersions(2), Some(("start", "stop"))),
      Column("d", PrefixQualifier("four"), QueryVersions(3)),
      Column("d", EmptyPrefixQualifier(), QueryVersions.All, Some(("start", "stop"))),
      Column("e", EmptyPrefixQualifier()),
      Column("f", PrefixQualifier("c")))
    val expectedRowConstraint = SingleRowConstraint("=", "id")
    val parserVal = parser
    val expectedResult = new parserVal.~(new parserVal.~(expectedColumns, tableName), Some(expectedRowConstraint))

    this.builder.getQueryOperation should be ('empty)
    this.builder.getTableName should be (null)
    this.builder.getColumns should be ('empty)
    this.builder.getRowConstraints should be ('empty)
    runSuccessfulParse[Any](parser, parser.query, query, expectedResult)
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getTableName should equal (tableName)
    this.builder.getColumns should not be ('empty)
    this.builder.getColumns should equal (expectedColumns.reverse)
    this.builder.getRowConstraints should not be ('empty)
    this.builder.getRowConstraints.head should equal (expectedRowConstraint)
  }

  @Test
  def testMinimalQueryMatches() {
    val query = "scan from table"

    val parserVal = parser
    val expectedResult = new parserVal.~(new parserVal.~(List(), "table"), None)

    this.builder.getQueryOperation should be ('empty)
    this.builder.getTableName should be (null)
    this.builder.getColumns should be ('empty)
    this.builder.getRowConstraints should be ('empty)
    runSuccessfulParse[Any](parser, parser.query, query, expectedResult)
    this.builder.getQueryOperation.get should equal (QueryOperation.Scan)
    this.builder.getTableName should equal ("table")
    this.builder.getColumns should be ('empty)
    this.builder.getRowConstraints should be ('empty)
  }

  @Test
  def testInvalidQueryFromOnly() {
    val query = "from table"
    runFailedParse[Any](parser, parser.query, query)
  }

  @Test
  def testInvalidQuerySelectAndWhere() {
    val query = "scan where rowkey = {id}"
    runFailedParse[Any](parser, parser.query, query)
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
