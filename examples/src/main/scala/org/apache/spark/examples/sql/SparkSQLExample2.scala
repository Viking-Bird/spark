package org.apache.spark.examples.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.{After, Test}

/**
  * SparkSQL测试
  *
  * @author pengwang
  * @date 2020/07/03
  */
class SparkSQLExample2 {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLExample2")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  @Test
  def testDataFrame: Unit = {
    // 创建一个DataFrame
    val df = spark.read.json("src/main/resources/people.json")
    df.show()

    // 创建一个临时视图当做表一样的来查询
    df.createTempView("student")
    this.spark.sql("select avg(age) from student").show
  }

  @Test
  def testTransform: Unit = {
    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = this.spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangfu", 40)))
    // 转换为DF
    val dataFrame: DataFrame = rdd.toDF("id", "name", "age")
    // 转换为DS
    val dataset: Dataset[User] = dataFrame.as[User]
    // DS转换为DF
    val dataFrame1: DataFrame = dataset.toDF()
    // DF转换为RDD
    val rdd1: RDD[Row] = dataFrame1.rdd

    rdd1.foreach(row => {
      // 获取数据时，通过索引访问数据
      println(row.getString(1))
    })
  }

  @Test
  def test1: Unit ={

  }

  @After
  def close: Unit = {
    this.spark.stop()
  }

}

case class User(id: Int, name: String, age: Int)

