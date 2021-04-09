package graph

import org.apache.spark.sql.types._
import io.prophecy.libs._
import io.prophecy.libs.UDFUtils._
import io.prophecy.libs.Component._
import io.prophecy.libs.DataHelpers._
import io.prophecy.libs.SparkFunctions._
import io.prophecy.libs.FixedFileFormatImplicits._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import graph._

@Visual(id = "Aggregate0", label = "Aggregate0", x = 687, y = 189, phase = 0)
object Aggregate0 {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("customer_id").as("customer_id"))
    val out = dfGroupBy.agg(
      max(col("first_name")).as("first_name"),
      max(col("last_name")).as("last_name"),
      max(col("phone")).as("phone"),
      sum(col("amount")).as("sum_amounts"),
      max(col("order_date")).as("order_date"),
      max(col("order_status")).as("order_status"),
      max(col("order_category")).as("order_category"),
      max(col("customer_id")).as("customer_id"),
      max(col("full_name")).as("full_name")
    )

    out

  }

}
