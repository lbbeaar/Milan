package training.domain

import org.apache.spark.sql.types._

object Pollution_mi extends Enumeration{
  private val DELIMITER = ";"

  val SENSOR_ID, TIME, MEASUREMENT = Value

  val structType: StructType = StructType(
    Seq(
      StructField(SENSOR_ID.toString, IntegerType),
      StructField(TIME.toString, StringType),
      StructField(MEASUREMENT.toString, FloatType)
    )
  )
}
