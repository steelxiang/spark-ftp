package loaddata


import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import org.apache.commons.io.FileUtils
import org.apache.commons.net.util.Base64
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable._

/**
  * @author xiang
  *         2018/11/15
  */


class ftp2hdfs_js{

  val logger = Logger.getLogger(classOf[ftp2hdfs_js])

}
object ftp2hdfs_js {

  val js = new ftp2hdfs_js
  val host = "172.31.20.172"
  val userName = "root"
  val password = "Hg!35#89s"
  val port = 22
  var path = "/data1/Data/ApkUrlData/apkData_JS"
  val localpath="/home/misas_dev/data2hdfs/tmp/"

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ftp-js")
    .config("spark.shuffle.consolidateFiles", true)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.debug.maxToStringFields", 100)
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val context = spark.sparkContext
  context.setLogLevel("WARN")
  val sqlcontext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    val flag = true

      js.logger.warn("读取列表")

    spark.read.text("d:\\cdpi-20181121.txt.gz").show()
    //   val list = getList
    //   list.foreach(t=>println(t))
   // upload(list)
    spark.close()
  }

  def upload(list: ListBuffer[String]): Unit = {

    var result:DataFrame=null
    for (filename <- list) {
      js.logger.warn("开始加载： "+filename)
      val df: DataFrame = spark.read.
        format("com.springml.spark.sftp").
        option("host", host).
        option("username", userName).
        option("password", password).
        option("fileType", "txt").
        option("port", port).
        load(path +"/" + filename)

      //DES2018111315593351126FX014.txt
      val namedate = filename.substring(3, 11)

          val frame = df.map(t => {

            val dataSource: Int = 12
            val URL: String =t.getString(0)
            val Id: String = ""
            val URL_Time: Int =1
            val dt: String = namedate
            YHtable(dataSource, URL, Id, URL_Time, dt)

          }
          )

     val table: DataFrame = frame.withColumn("date",to_date(unix_timestamp($"dt","yyyyMMdd").cast("timestamp"),"yyyyMMdd")).drop("dt")

      //  source_ds.show()
      // table.show()
      js.logger.warn("开始插入： "+filename)
      table.write.mode("append").insertInto("url.apk")
      js.logger.warn("插入完成： "+filename)
      FileUtils.deleteQuietly(new File("/tmp/"+filename))
      js.logger.warn("删除临时文件： "+filename)

    }
    js.logger.warn("----------list---finish-------------")


  }


  //获取目录列表
  def getList = {

    try {
      val client = new SFTPUtil(userName, password, host, port)
      client.login()
      val filelist: util.Vector[_] = client.listFiles(path)
      val list: ListBuffer[String] = ListBuffer()
      for (i <- 0 until filelist.size()) {
        val str = filelist.get(i).toString.split("\\s+").last
        if (str.endsWith(".txt")) {
          list.append(str)

        }
      }
      client.logout()

      list
    }
    catch {
      case ex: SftpException => throw new Exception("listFiles exception")
    }

  }









}












































































