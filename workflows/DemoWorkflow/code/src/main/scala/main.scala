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

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_Customers:     Source    = Customers(spark)
    val df_Orders:        Source    = Orders(spark)
    val df_By_CustomerId: Join      = By_CustomerId(spark, df_Customers, df_Orders)
    val df_FullName:      Reformat  = FullName(spark,      df_By_CustomerId)
    val df_Aggregate0:    Aggregate = Aggregate0(spark,    df_FullName)
    CustomersOrders(spark, df_Aggregate0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark = SparkSession.builder().appName("DemoWorkflow").enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
