package MovieCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * *  3，"movies.dat"：MovieID::Title::Genres
 * *  4, "occupations.dat"：OccupationID::OccupationName
 *
 * 需求1
 * 1.读取信息,统计数据条数 : 职业数,电影数,用户数,评分条数
 * 2.显示每个职业下的用户详细信息
 * 显示为: (职业编号,( 人的编号,性别,年龄,邮编),职业名)
 * 完成
 *
 **/
object Test1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()
  val sql = session.sqlContext

  import session.implicits._

  /*session.read.textFile("src/main/data/movies.dat").map(line=>{
    var x = line.split("::")
    (x(0),x(1),x(2))
  }).toDF("ID","NAME","TYPE").show()*/

  session.read.textFile("src/main/data/users.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2), x(3), x(4))
  }).toDF("ID", "GENDER", "AGE", "OCCUPATION", "CODE").createTempView("user")

  session.read.textFile("src/main/data/occupation.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1))
  }).toDF("ID", "NAME").createTempView("occupation")

  session.sql("select occupation.ID occupid,user.ID userid,GENDER,AGE,CODE,occupation.NAME from user right join occupation where user.OCCUPATION=occupation.ID").show()
}
