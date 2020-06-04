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
  //调整日志输出级别，只输出ERROR信息
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession //spark2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  //读取用户信息
  var userDF = session.read.textFile("src/main/data/users.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1), x(2), x(3), x(4))
  }).toDF("id", "gender", "age", "occupation", "code") //设置列名
  //并创建视图
  userDF.createTempView("user")

  //读取职业信息
  var occupationDF = session.read.textFile("src/main/data/occupation.dat").map(line => {
    var x = line.split("::")
    (x(0), x(1))
  }).toDF("oid", "name")
  occupationDF.createTempView("occupation")

  //SQL查询方案
  //联立查询
  session.sql("select occupation.oid occupid,user.id userid,gender,age,code,occupation.name from " +
    "user inner join occupation on user.occupation=occupation.oid").show()

  //API方案

  userDF.join(occupationDF,$"occupation"===$"oid").select("oid","id","gender","age","occupation","code","name").show(20)
}
