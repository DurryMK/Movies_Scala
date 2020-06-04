import app1.{session, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object app2 extends  App{
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app2")
    .master("local[*]")
    .getOrCreate()

  var sql = session.sqlContext

  var noCase = sql.sparkContext.textFile("src/main/data/movies.dat").map(line => { //使用StructType
    Row(line.split("::")(0).toInt, line.split("::")(1), line.split("::")(2))
  })
  val schema = StructType(List(
    StructField("id", IntegerType, false),
    StructField("name", StringType, false),
    StructField("type", StringType, false)
  ))

  var result = session.createDataFrame(noCase,schema)

  result.show()
}
