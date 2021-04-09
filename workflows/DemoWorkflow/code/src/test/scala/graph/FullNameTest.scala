
package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, when}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.junit.Assert
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

@RunWith(classOf[JUnitRunner])
class FullNameTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Unit test 0 for out columns: full_name") {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    val dfIn = inDf(Seq("first_name", "last_name"), Seq(
      Seq[Any]("John","Smith")
    ))

    val dfOut = outDf(Seq("full_name"), Seq(
      Seq[Any]("John - Smith")
    ))

    val dfOutComputed = graph.FullName(spark, dfIn)
    assertDFEquals(dfOut.select("full_name"), dfOutComputed.select("full_name"))
  }

  def inDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
      "phone" -> "",
      "amount" -> "",
      "order_date" -> "",
      "order_status" -> "",
      "order_category" -> "",
      "order_id" -> "",
      "customer_id" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "first_name" -> StringType,
      "last_name" -> StringType,
      "phone" -> StringType,
      "amount" -> StringType,
      "order_date" -> StringType,
      "order_status" -> StringType,
      "order_category" -> StringType,
      "order_id" -> StringType,
      "customer_id" -> StringType
    )

    val loweredColumns  = columns.map(_.toLowerCase)
    val missingColumns  = (defaults.keySet -- columns.toSet).toList.filter(x => !loweredColumns.contains(x))
    val allColumns      = columns ++ missingColumns
    val allValues       = values.map { row => row ++ missingColumns.map(column => defaults(column)) }

    val rdd    = spark.sparkContext.parallelize(allValues.map(values => Row.fromSeq(values)))
    val schema = StructType(allColumns.map(column => StructField(column, columnToTypeMap.getOrElse(column, StringType))))
    val df     = spark.createDataFrame(rdd, schema)

    nullify(df)
  }

  def outDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
      "phone" -> "",
      "amount" -> "",
      "order_date" -> "",
      "order_status" -> "",
      "order_category" -> "",
      "order_id" -> "",
      "customer_id" -> "",
      "full_name" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "first_name" -> StringType,
      "last_name" -> StringType,
      "phone" -> StringType,
      "amount" -> StringType,
      "order_date" -> StringType,
      "order_status" -> StringType,
      "order_category" -> StringType,
      "order_id" -> StringType,
      "customer_id" -> StringType,
      "full_name" -> StringType
    )

    val loweredColumns  = columns.map(_.toLowerCase)
    val missingColumns  = (defaults.keySet -- columns.toSet).toList.filter(x => !loweredColumns.contains(x))
    val allColumns      = columns ++ missingColumns
    val allValues       = values.map { row => row ++ missingColumns.map(column => defaults(column)) }

    val rdd    = spark.sparkContext.parallelize(allValues.map(values => Row.fromSeq(values)))
    val schema = StructType(allColumns.map(column => StructField(column, columnToTypeMap.getOrElse(column, StringType))))
    val df     = spark.createDataFrame(rdd, schema)

    df
  }

  class StringColumnExtensions(val s: String) {
    def toDate: Date =
      Date.valueOf(s)

    def toDateTime: Timestamp =
      Timestamp.valueOf(LocalDateTime.parse(s.substring(0, Math.min(23, s.size))))
  }

  private def nullify(df: DataFrame): DataFrame = {
    val exprs = df.schema.map { f =>
      f.dataType match {
        case StringType =>
          when(
            col(f.name) === "null",
            lit(null: String).cast(StringType)).otherwise(col(f.name)
          ).as(f.name)
        case _ => col(f.name)
      }
    }

    df.select(exprs: _*)
  }

  def postProcess(origDf: DataFrame) : DataFrame = {
    val df = origDf.na.fill("null")
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = true))))
  }

  /**
    * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance, should be less than 1.
    */
  def assertDFEquals(expectedUnsorted: DataFrame, resultUnsorted: DataFrame, tol: Double = 0.0) {
    def _sort(df:      DataFrame) = df.orderBy(df.columns.map(col):_*)
    def fetchTypes(df: DataFrame) = df.schema.fields.map(f => (f.name, f.dataType))

    def _assertEqualsTypes(dfExpected: DataFrame, dfActual: DataFrame): Unit = {
      val expectedTypes = fetchTypes(dfExpected)
      val actualTypes   = fetchTypes(dfActual)

      // assertion
      Assert.assertTrue(
        s"Types NOT equal! ${expectedTypes.mkString(",")}	!=	${actualTypes.mkString(",")}",
        expectedTypes.diff(actualTypes).isEmpty
      )
    }

    def _assertEqualsCount(dfExpected: DataFrame, dfActual: DataFrame): Unit =
      Assert.assertEquals(
        s"Length not Equal." +
          s"\nExpected: ${dfExpected.take(maxUnequalRowsToShow).mkString(",")}" +
          s"\nActual: ${dfActual.take(maxUnequalRowsToShow).mkString(",")}",
        dfExpected.rdd.count,
        dfActual.rdd.count
      )

    def _assertEqualsValues(dfExpected: DataFrame, dfActual: DataFrame): Unit = {
      val expectedVal = dfExpected.rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
      val resultVal   = dfActual.rdd.zipWithIndex().map { case (row, idx) => (idx, row) }

      val unequalRDD = expectedVal
        .join(resultVal)
        .filter { case (idx, (r1, r2)) => !SparkTestingUtils.rowEquals(r1, r2, tol) }

      Assert.assertEquals(
        s"Expected != Actual\nMismatch: ${unequalRDD.take(maxUnequalRowsToShow).mkString("    +    ")}",
        unequalRDD.take(maxUnequalRowsToShow).length,
        0
      )
    }

    val expected = _sort(postProcess(expectedUnsorted))
    val result   = _sort(postProcess(resultUnsorted))

    try {
      expected.rdd.cache
      result.rdd.cache

      // assert row count
      _assertEqualsCount(expected, result)

      // assert matching types
      _assertEqualsTypes(expected, result)

      // assert matching values for all rows * columns
      _assertEqualsValues(expected, result)

    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }
}

