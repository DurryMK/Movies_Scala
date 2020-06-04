package MovieCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * *  3，"movies.dat"：MovieID::Title::Genres
 * *  4, "occupations.dat"：OccupationID::OccupationName
 *
 * * 需求2
 * * * 1.某个用户看过的电影数量
 * * * 2.这些电影的信息，格式为: (Movield,Title,Genres)
 * *完成
 ***/
object Test2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()
  val sql = session.sqlContext

  import session.implicits._

  /*

  session.read.textFile("src/main/data/users.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2), x(3), x(4))
  }).toDF("ID", "GENDER", "AGE", "OCCUPATION", "CODE").createTempView("user")

  session.read.textFile("src/main/data/occupation.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1))
  }).toDF("ID", "NAME").createTempView("occupation")
  */

  session.read.textFile("src/main/data/movies.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2))
  }).toDF("ID", "NAME", "TYPE").createTempView("movies")

  session.read.textFile("src/main/data/ratings.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2),x(3))
  }).toDF("userid", "movieid", "rating","time").createTempView("rating")
  var count = session.sql("select ID movieid,NAME title,TYPE type from movies where ID in (select movieid from rating where userid=1)").count()
  print(count)

}
