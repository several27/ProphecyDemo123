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

@Visual(id = "Customers", label = "Customers", x = 151, y = 111, phase = 0)
object Customers {

  @UsesDataset(id = "1466", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val fabric = Config.fabricName

    val out = fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType,   true),
            StructField("first_name",        StringType,    true),
            StructField("last_name",         StringType,    true),
            StructField("phone",             StringType,    true),
            StructField("email",             StringType,    true),
            StructField("country_code",      StringType,    true),
            StructField("account_open_date", TimestampType, true),
            StructField("account_flags",     StringType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/aayushman@prophecy.io/CustomersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric '$fabric' is not handled")
    }

    out

  }

}
