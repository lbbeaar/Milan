package training.domain
import org.apache.spark.sql.types._

object Pollution_mi_legend extends Enumeration{
  private val DELIMITER = ";"

  val SENSOR_ID, STREET_NAME, SENSOR_LAT, SENSOR_LONG, SENSOR_TYPE, UOM, LTIME = Value

  val structType: StructType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(STREET_NAME.toString, StringType),
      StructField(SENSOR_LAT.toString, DoubleType),
      StructField(SENSOR_LONG.toString, DoubleType),
      StructField(SENSOR_TYPE.toString, StringType),
      StructField(UOM.toString, StringType),
      StructField(LTIME.toString, StringType)
    )
  )
}
