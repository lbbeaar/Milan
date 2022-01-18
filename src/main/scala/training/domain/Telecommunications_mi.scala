package training.domain
import org.apache.spark.sql.types._

object Telecommunications_mi extends Enumeration{
  private val DELIMITER = "\t"

  val SQUARE_ID, TIME_INTERVAL, COUNTRY_CODE, SMS_IN, SMS_OUT, CALL_IN, CALL_OUT, INTERNET  = Value

  val structType: StructType = StructType(
    Seq(
      StructField(SQUARE_ID.toString, IntegerType),
      StructField(TIME_INTERVAL.toString, LongType),
      StructField(COUNTRY_CODE.toString, IntegerType),
      StructField(SMS_IN.toString, DoubleType),
      StructField(SMS_OUT.toString, DoubleType),
      StructField(CALL_IN.toString, DoubleType),
      StructField(CALL_OUT.toString, DoubleType),
      StructField(INTERNET.toString, DoubleType)
    )
  )
}
