package loaddata

import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiang
  * 2018/11/15
  */
object fpt2hdfs {


  val spark: SparkSession = SparkSession
    .builder()
    .appName("yangmaodang")
    .config("spark.shuffle.consolidateFiles", true)
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  //读取数据  192.168.5.202:22
  val context = spark.sparkContext
  context.setLogLevel("WARN")
  val sqlcontext = spark.sqlContext
  def main(args: Array[String]): Unit = {

    val df: DataFrame = spark.read.
      format("com.springml.spark.sftp").
      option("host", "192.168.5.200").
      option("username", "root").
      option("password", "123456").
      option("fileType", "txt").
      option("port", "22").
      load("/root/001.txt")
    df.show()
val dataSource="ftp://root:123456/192.168.5.200/root/001.txt"

  //  context.addFile(dataSource)
  // val str = SparkFiles.get("001.txt")

   // frame.foreach(t=>println(t))
  //  val frame = spark.read.text(str)
    df.show()



  }
}












































































