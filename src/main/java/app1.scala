import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object app1 extends App {

  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app1")
    .master("local[*]")
    .getOrCreate()
  var sc = session.sparkContext

  var sql = session.sqlContext

  import session.implicits._

  var context = sql.read.textFile("src/main/data/movies.dat").map(line => { //使用样例类
    var id = line.split("::")(0).toInt
    var name = line.split("::")(1)
    var clazz = line.split("::")(2)
    Movie(id, name, clazz)
  }).toDF()

  context.createTempView("vmovies")
  session.sql("select * from vmovies where id<=200").show()
  println(context.count())

  var noCase = sql.sparkContext.textFile("src/main/data/movies.dat").map(line => { //使用StructType
    Row(line.split("::")(0).toInt, line.split("::")(1), line.split("::")(2))
  })
  val schema = StructType(List(
    StructField("id", IntegerType, false),
    StructField("name", StringType, false),
    StructField("type", StringType, false)
  ))

}

case class Movie(id: Int, name: String, clazz: String)
