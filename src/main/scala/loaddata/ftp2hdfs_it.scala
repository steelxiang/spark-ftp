package loaddata


import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import org.apache.commons.net.util.Base64
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable._

/**
  * @author xiang
  *         2018/11/15
  */

object ftp2hdfs_it {
  val host = "10.4.41.99:21"
  val userName = "yanjy_lch"
  val password = "UxGsD1a#,kA"
  val port = "21"
  var path = "/data/yhb/pdc_in"  ///data/yhb/url_in


  val spark: SparkSession = SparkSession
    .builder()
    .appName("yangmaodang")
    .config("spark.shuffle.consolidateFiles", true)
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val context = spark.sparkContext
  context.setLogLevel("WARN")
  val sqlcontext = spark.sqlContext
  val date = getToday
  val p=""
  def main(args: Array[String]): Unit = {


    upload(p,s"lte_cdpi_url_$date.txt.gzip", 6)
    upload(p,s"3g_cdpi_url_$date.txt.gzip", 7)
    upload(p,s"lte-$date.txt.gzip", 8)
    upload(p,s"cdpi-$date.txt.gzip", 9)
    upload(p,s"gdpi-$date.txt.gzip", 10)
    upload(p,s"gdpi_url_$date.txt.gzip", 11)

    spark.close()
  }


  def upload(p:String ,filename: String, dataType: Int): Unit = {
    val df = spark.read.text(s"ftp://$userName:$password@$host$password$p$filename")
    println(filename)
    val value: Dataset[Array[String]] = df.map(t => t.getString(0).split("\t")).filter(t => t.length == 2)
    val table = value.map(words => {

      val dataSource: Int = dataType
      val URL: String = words(0)
      val Id: String = ""
      val URL_Time: Int = words(0).toInt
      val dt: String = date
      YHtable(dataSource, URL, Id, URL_Time, dt)

    }
    )

    table.show()
    // table.write.partitionBy("date").insertInto("dpi")

  }

  println("----------list---finish-------------")


  //获取当天日期
  def getToday: String = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val s = dateFormat.format(calendar.getTime)
    println("today is : " + s)
    s
  }


}












































































