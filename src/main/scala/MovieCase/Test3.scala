package MovieCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * *  3，"movies.dat"：MovieID::Title::Genres
 * *  4, "occupations.dat"：OccupationID::OccupationName
 *
 * 需求3
 * * * 1.所有电影中平均得分最高的前10部电影: ( 分数,电影ID)
 * * * 2.观看人数最多的前10部电影: (观影 人数,电影ID)
 * 完成
 */
object Test3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()
  val sql = session.sqlContext

  import session.implicits._

  /*
session.read.textFile("src/main/data/movies.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2))
  }).toDF("ID", "NAME", "TYPE").createTempView("movies")
  session.read.textFile("src/main/data/users.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2), x(3), x(4))
  }).toDF("ID", "GENDER", "AGE", "OCCUPATION", "CODE").createTempView("user")

  session.read.textFile("src/main/data/occupation.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1))
  }).toDF("ID", "NAME").createTempView("occupation")
  */



  session.read.textFile("src/main/data/ratings.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2),x(3))
  }).toDF("userid", "movieid", "rating","time").createTempView("ratings")
  /*session.sql("select avg(rating) avg,movieid id from ratings group by ratings.movieid order by avg desc limit 10").show()*/
  /**
   * |avg|  id|
   * +---+----+
   * |5.0|3233|
   * |5.0|1830|
   * |5.0|3881|
   * |5.0| 787|
   * |5.0|3172|
   * |5.0|3607|
   * |5.0|3656|
   * |5.0|3280|
   * |5.0| 989|
   * |5.0|3382|
   * +---+----+
   * */

  var count = session.read.textFile("src/main/data/ratings.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2),x(3))
  }).filter(_._2.toInt==260).count()

  print(count)
  /*session.sql("select count(movieid) num,movieid id  from ratings group by movieid order by num desc" ).show()*/

  /**
   * | num|  id|
   * +----+----+
   * |3428|2858|
   * |2991| 260|
   * |2990|1196|
   * |2883|1210|
   * |2672| 480|
   * |2653|2028|
   * |2649| 589|
   * |2590|2571|
   * |2583|1270|
   * |2578| 593|
   * |2538|1580|
   * |2514|1198|
   * |2513| 608|
   * |2459|2762|
   * |2443| 110|
   * |2369|2396|
   * |2318|1197|
   * |2304| 527|
   * |2288|1617|
   * |2278|1265|
   * +----+----+
   * */
}
