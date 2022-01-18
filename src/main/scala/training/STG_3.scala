package training

import org.apache.spark.sql.functions.{from_unixtime, hour, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters

object STG_3 extends App{
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("STG_3")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  Parameters.initTables

  val legend_table: DataFrame = spark.table("pollution_mi_legend")
  val pollution_mi_table: DataFrame = spark.table("pollution_mi")

  val stg3_tbl: DataFrame = legend_table.join(pollution_mi_table, Seq("sensor_id"), "inner")
    .withColumn("time_interval",hour(to_timestamp('time, "yyyy/MM/dd HH:mm")))
    .select('sensor_id, 'street_name, 'sensor_lat, 'sensor_long, 'sensor_type, 'time_interval, 'measurement)
    .filter('time_interval.between(9, 13) or 'time_interval.between(15, 17))
  stg3_tbl.show(false)
}
