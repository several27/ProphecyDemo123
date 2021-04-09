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

@Visual(id = "CustomersOrders", label = "CustomersOrders", x = 683, y = 190, phase = 0)
object CustomersOrders {

  @UsesDataset(id = "1468", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("first_name",     StringType,    false),
            StructField("last_name",      StringType,    false),
            StructField("phone",          StringType,    false),
            StructField("amount",         DoubleType,    false),
            StructField("order_date",     TimestampType, false),
            StructField("order_status",   StringType,    false),
            StructField("order_category", StringType,    false),
            StructField("order_id",       IntegerType,   false),
            StructField("customer_id",    IntegerType,   false),
            StructField("full_name",      StringType,    false)
          )
        )
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("dbfs:/Prophecy/aayushman@prophecy.io/CustomersOrdersOutput.parq")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
