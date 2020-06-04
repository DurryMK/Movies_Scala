import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object nm extends App {
  val session = SparkSession
    .builder()
    .appName("sessionTest")
    .master("local[*]")
    .getOrCreate()
  val sqlCon = session.sqlContext

  import sqlCon.implicits._

  session.sparkContext
    .textFile("D://Person.txt")
    .map(line => {
      var x = line.split(",")
      Person(x(0).toInt, x(1), x(2).toInt, x(3).toDouble)
    }).toDF()
    .createTempView("Person")
  sqlCon.sql("select * from Person where id = 2").show()

}

case class Person(id: Int, name: String, age: Int, height: Double)