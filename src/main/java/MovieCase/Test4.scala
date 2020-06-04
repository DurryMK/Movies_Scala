package MovieCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * *  3，"movies.dat"：MovieID::Title::Genres
 * *  4, "occupations.dat"：OccupationID::OccupationName
 *
 * 需求6:分析不同类型的电影总数
 ***/
object Test4 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()
  val sql = session.sqlContext

  import session.implicits._

  session.read.textFile("src/main/data/movies.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2))
  }).toDF("ID", "NAME", "TYPE").createTempView("movies")


}
