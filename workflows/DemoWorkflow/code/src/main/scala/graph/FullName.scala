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

@Visual(id = "FullName", label = "FullName", x = 541, y = 188, phase = 0)
object FullName {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("amount"),
      col("order_date"),
      col("order_status"),
      col("order_category"),
      col("order_id"),
      col("customer_id"),
      concat(col("first_name"), col("last_name")).as("full_name")
    )

    out

  }

}
