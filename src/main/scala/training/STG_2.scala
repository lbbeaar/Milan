package training

import org.apache.spark.sql.functions.{avg, from_unixtime, hour, max, min, when, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters

object STG_2 extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("STG_2")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  Parameters.initTables

  val grid = spark.read.json("C:\\Users\\admedvedeva\\Desktop\\Milan\\dataset\\milano-grid\\milano-grid.geojson")

  val table_tele: DataFrame = spark.table("telecommunications_mi")
    .withColumn("time_interval", hour(from_unixtime('time_interval/1000L)))
    .select('square_id, 'time_interval, 'country_code, 'sms_in, 'sms_out, 'call_in, 'call_out, 'internet)
    .filter('time_interval.between(9, 13) or 'time_interval.between(15, 17))
    .groupBy('square_id)
    .agg(avg('sms_in + 'sms_out + 'call_in + 'call_out + 'internet) as "avg_traffic")
    .select('square_id, 'avg_traffic, avg('avg_traffic).over() as "all_avg",
      min('avg_traffic).over() as "min_traffic", max('avg_traffic).over() as "max_traffic")
    .withColumn("type_zone", when('all_avg > 'avg_traffic, "Home").otherwise("Work"))

  val features = grid.select(explode('features) as "features")
  val coordinates = features.select($"features.properties.cellId" as "square_id",
                                    explode($"features.geometry.coordinates") as "coordinates")
    .withColumn("min_long", $"coordinates".getItem(0).getItem(0))
    .withColumn("max_lat", $"coordinates".getItem(0).getItem(1))
    .withColumn("max_long", $"coordinates".getItem(2).getItem(0))
    .withColumn("min_lat", $"coordinates".getItem(2).getItem(1))
  val grid_mi = coordinates.select('square_id, 'min_long, 'max_long, 'min_lat, 'max_lat)
  val stg2_tbl = table_tele.join(grid_mi, Seq("square_id"), "inner")
  coordinates.show(false)
  stg2_tbl.orderBy('square_id).show(false)
}
