package training

import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}


object pgTest2 extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("pgSQL")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val ids: DataFrame = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/cinema")
    .option("dbtable", "public.ages")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()

  val countIds = ids.agg(max("id"), min("id"))
  val maxId = countIds.select("max(id)").head().getDecimal(0).toBigInteger.toString.toInt
  val minId = countIds.select("min(id)").head().getDecimal(0).toBigInteger.toString.toInt
  println(maxId)
  print(minId)

  val ages: DataFrame = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/cinema")
    .option("dbtable", "public.ages")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("partitionColumn", "id")
    .option("lowerBound", minId)
    .option("upperBound", maxId)
    .option("numPartitions", 4)
    .load()
    spark.time(ages.show(10063))
}
