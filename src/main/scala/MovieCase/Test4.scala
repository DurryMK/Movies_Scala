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

  session.read.textFile("src/main/data/movies.dat")
    .map(line => {
      var x = line.split("::")
      (x(0), x(1), x(2))
    })
    .flatMap(x => { //分解第三条数据 电影类型
      x._3.split("\\|")
    }).toDF("type") //取列名
    .createTempView("types") //创建视图

  //SQL方案
  session.sql("select count(*) num,type from types group by type order by num desc").show()
/**
 * | num|       type|
 * +----+-----------+
 * |1603|      Drama|
 * |1200|     Comedy|
 * | 503|     Action|
 * | 492|   Thriller|
 * | 471|    Romance|
 * | 343|     Horror|
 * | 283|  Adventure|
 * | 276|     Sci-Fi|
 * | 251| Children's|
 * | 211|      Crime|
 * | 143|        War|
 * | 127|Documentary|
 * | 114|    Musical|
 * | 106|    Mystery|
 * | 105|  Animation|
 * |  68|    Fantasy|
 * |  68|    Western|
 * |  44|  Film-Noir|
 * +----+-----------+
 * */
  //API方案


}
