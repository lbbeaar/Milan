package training

import org.apache.spark.sql.functions.{avg, from_unixtime, hour, max, min, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters

object STG_1 extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("STG_1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  Parameters.initTables

  val table_tele: DataFrame = spark.table("telecommunications_mi")
    .withColumn("time_interval", hour(from_unixtime('time_interval/1000L)))
    .select('square_id, 'time_interval, 'country_code, 'sms_in, 'sms_out, 'call_in, 'call_out, 'internet)
    .filter('time_interval.between(9, 13) or 'time_interval.between(15, 17))
    .groupBy('square_id)
    .agg(avg('sms_in + 'sms_out + 'call_in + 'call_out + 'internet) as "avg_traffic")
    .select('square_id, 'avg_traffic, avg('avg_traffic).over() as "all_avg",
      min('avg_traffic).over() as "min_traffic", max('avg_traffic).over() as "max_traffic")
    .withColumn("type_zone", when('all_avg > 'avg_traffic, "Home").otherwise("Work"))
  table_tele.show()
}
