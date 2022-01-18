package training.system

import training.domain._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Parameters {
  val POPULATION_DATASET_PATH = ""
  val EXAMPLE_OUTPUT_PATH = "./output/"


  val path_telecommunication = "./dataset/test/*"
//  val path_telecommunication = "./dataset/telecommunications_mi/*"
  val path_legend = "./dataset/pollution-legend-mi.csv"
  val path_pollution = "./dataset/pollution-mi/*"

  val table_telecommunication = "telecommunications_mi"
  val table_legend = "pollution_mi_legend"
  val table_pollution = "pollution_mi"

  private def createTable(name: String, structType: StructType, path: String, delimiter: String = "\t")
                         (implicit spark: SparkSession): Unit = {
    spark.read
      .format("com.databricks.spark.csv")
        //.option("inferSchema", "true")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N",
          "dateFormat" -> "yyyy/MM/dd HH:mm"
        )
      ).schema(structType).load(path).createOrReplaceTempView(name)
  }

  private def createTableCSV(name: String, structType: StructType, path: String, delimiter: String = ",")
                         (implicit spark: SparkSession): Unit = {
    spark.read
      .format("com.databricks.spark.csv")
      //.option("inferSchema", "true")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N",
          "dateFormat" -> "yyyy/MM/dd HH:mm"
        )
      ).schema(structType).load(path).createOrReplaceTempView(name)
  }

  def initTables(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_telecommunication, Telecommunications_mi.structType, Parameters.path_telecommunication)
    createTableCSV(Parameters.table_legend, Pollution_mi_legend.structType, Parameters.path_legend)
    createTableCSV(Parameters.table_pollution, Pollution_mi.structType, Parameters.path_pollution)
  }
}
