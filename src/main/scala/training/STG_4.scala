package training

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, explode, from_unixtime, hour, lit, max, min, row_number, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import training.system.Parameters

object STG_4 extends App{
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("STG_3")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  Parameters.initTables

  val grid = spark.read.json("C:\\Users\\admedvedeva\\Desktop\\Milan\\dataset\\milano-grid\\milano-grid.geojson")
  val legend_table: DataFrame = spark.table("pollution_mi_legend")
  val pollution_mi_table: DataFrame = spark.table("pollution_mi")
  /*STG_1*/
  val table_tele: DataFrame = spark.table("telecommunications_mi")
    .withColumn("time_interval", hour(from_unixtime('time_interval/1000L)))
    .select('square_id, 'time_interval, 'country_code, 'sms_in, 'sms_out, 'call_in, 'call_out, 'internet)
    .filter('time_interval.between(9, 13) or 'time_interval.between(15, 17))
    .groupBy('square_id)
    .agg(avg('sms_in + 'sms_out + 'call_in + 'call_out + 'internet) as "avg_traffic")
    .select('square_id, 'avg_traffic, avg('avg_traffic).over() as "all_avg",
      min('avg_traffic).over() as "min_traffic", max('avg_traffic).over() as "max_traffic")
    .withColumn("type_zone", when('all_avg > 'avg_traffic, "Home").otherwise("Work"))

  /*STG_2*/
  val features = grid.select(explode('features) as "features")
  val coordinates = features.select($"features.properties.cellId" as "square_id",
    explode($"features.geometry.coordinates") as "coordinates")
    .withColumn("min_long", $"coordinates".getItem(0).getItem(0))
    .withColumn("max_lat", $"coordinates".getItem(0).getItem(1))
    .withColumn("max_long", $"coordinates".getItem(2).getItem(0))
    .withColumn("min_lat", $"coordinates".getItem(2).getItem(1))
  val grid_mi = coordinates.select('square_id, 'min_long, 'max_long, 'min_lat, 'max_lat)
  val stg2_tbl = table_tele.join(grid_mi, Seq("square_id"), "inner")

  /*STG_3*/
  val stg3_tbl: DataFrame = legend_table.join(pollution_mi_table, Seq("sensor_id"), "inner")
    .withColumn("time_interval", hour(to_timestamp('time, "yyyy/MM/dd HH:mm")))
    .select('sensor_id, 'street_name, 'sensor_lat, 'sensor_long, 'sensor_type, 'time_interval, 'measurement)
//    .filter('time_interval.between(9, 13) or 'time_interval.between(15, 17))

  /*STG_4*/
  val res_tbl: DataFrame = stg2_tbl.join(stg3_tbl,
    'sensor_long >= 'min_long and 'sensor_long <= 'max_long and
    'sensor_lat >= 'min_lat and 'sensor_lat <= 'max_lat,"inner")
  val w = Window.orderBy('avg_meas)
  val wd = Window.orderBy('avg_meas.desc)
  val measurement_table =
    res_tbl.groupBy('square_id)
      .agg(countDistinct('sensor_id) as "count_sensor", avg('measurement) as "avg_meas",
        min('measurement) as "min_meas", max('measurement) as "max_meas",
        max('min_traffic) as "min_traffic", max('max_traffic) as "max_traffic",
        max('type_zone) as "type_zone", max('street_name) as "street_name")
      .withColumn("row", row_number().over(w))
      .withColumn("count", max('row).over())
      .withColumn("min_meas_zone", when('row <= 5, 'avg_meas).otherwise(lit(" ")))
      .withColumn("max_meas_zone", when('row > 'count-5, 'avg_meas).otherwise(lit(" ")))
      .withColumn("other_meas_zone",
        when('row.between(('count/2)-2, ('count/2)+2), 'avg_meas).otherwise(lit(" ")))
      .drop('row)
      .drop('count)

  measurement_table.show()
}
