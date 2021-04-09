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

@Visual(id = "By_CustomerId", label = "By CustomerId", x = 377, y = 188, phase = 0)
object By_CustomerId {

  def apply(spark: SparkSession, left: DataFrame, right: DataFrame): Join = {
    import spark.implicits._

    val leftAlias  = left.as("left")
    val rightAlias = right.as("right")
    val dfJoin     = leftAlias.join(rightAlias, col("left.customer_id") === col("right.customer_id"), "inner")

    val out = dfJoin.select(
      col("left.first_name").as("first_name"),
      col("left.last_name").as("last_name"),
      col("left.phone").as("phone"),
      col("right.amount").as("amount"),
      col("right.order_date").as("order_date"),
      col("right.order_status").as("order_status"),
      col("right.order_category").as("order_category"),
      col("right.order_id").as("order_id"),
      col("left.customer_id").as("customer_id")
    )

    out

  }

}
