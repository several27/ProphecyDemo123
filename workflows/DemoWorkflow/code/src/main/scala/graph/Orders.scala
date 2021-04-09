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

@Visual(id = "Orders", label = "Orders", x = 148, y = 255, phase = 0)
object Orders {

  @UsesDataset(id = "1467", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType,   true),
            StructField("customer_id",    IntegerType,   true),
            StructField("order_status",   StringType,    true),
            StructField("order_category", StringType,    true),
            StructField("order_date",     TimestampType, true),
            StructField("amount",         DoubleType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/aayushman@prophecy.io/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
