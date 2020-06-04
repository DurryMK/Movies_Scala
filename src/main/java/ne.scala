import org.apache.spark.sql.SparkSession

object  ne{
  val session = SparkSession
    .builder()
    .appName("sessionTest")
    .master("local[*]")
    .getOrCreate()


}
