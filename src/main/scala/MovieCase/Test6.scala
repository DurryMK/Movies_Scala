package MovieCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * *  3，"movies.dat"：MovieID::Title::Genres
 * *  4, "occupations.dat"：OccupationID::OccupationName
 *
 * * 需求8:分析每年度不同类型的电影生产总数
***/
object Test6 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()
  val sql = session.sqlContext

  import session.implicits._

  session.read.textFile("src/main/data/movies.dat")
    .map(line => {
      var x = line.split("::")
      x(1).substring(x(1).lastIndexOf("(") + 1, x(1).lastIndexOf(")"))
    })
    .toDF("year")
    .createTempView("years")


  //SQL方案
  //API方案
}
