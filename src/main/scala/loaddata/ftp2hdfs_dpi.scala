package loaddata


import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import loaddata.ftp2hdfs_dpi._
import org.apache.commons.io.FileUtils
import org.apache.commons.net.util.Base64
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable._

/**
  * @author xiang
  *         2018/11/15
  */


case class tableData(commandId: Int,
                     src_ip: String,
                     src_port: Int,
                     dest_ip: String,
                     dest_port: Int,
                     request_time: String,
                     response_time: String,
                     domain: String,
                     url: String,
                     user_agent: String,
                     content_type: String,
                     content_size: String,
                     refer: String,
                     cookie: String,
                     method:String,
                     status: String,
                     proxy_type: String,
                     proxy_ip: String,
                     proxy_port: String,
                     link: String,
                     view: String,
                     transport_protocol: String,
                     access_time: String,
                     dataSource: Int,
                     dt: String)

class ftp2hdfs_dpi{

  val logger = Logger.getLogger(classOf[ftp2hdfs_dpi])

  //获取目录列表
  def getList(client:SFTPUtil,date: String) = {

    try {
      client.login()
      val filelist: util.Vector[_] = client.listFiles(s"$path$date")
      val list: ListBuffer[String] = ListBuffer()
      for (i <- 0 until filelist.size()) {
        val str = filelist.get(i).toString.split("\\s+").last
        if (!".".equals(str) && !"..".equals(str)) {
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

  //获取当天日期
  def getToday: String = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val s = dateFormat.format(calendar.getTime)
    println("today is : " + s)
    s
  }





}


object ftp2hdfs_dpi {
  val host = "180.100.230.178"
  val userName = "chenzs"
  val password = "Jiangsumisas"
  val port = 14333
  var path = "/home/opt/data/log/"
  val localpath="/home/misas_dev/"
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ftp-dpi")
    .config("spark.shuffle.consolidateFiles", true)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.debug.maxToStringFields", 100)
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val context = spark.sparkContext


  def main(args: Array[String]): Unit = {


    val dpi = new ftp2hdfs_dpi
    val client=new SFTPUtil(userName,password,host,port)

    val flag = true
    var readList = new util.ArrayList[String]() //存放当天已读过的文件列表
    var today = dpi.getToday

    while (flag) {
     dpi.logger.info("读取列表")
      var list: ListBuffer[String] = ListBuffer[String]()
      try {
        list = dpi.getList(client,today)
      } catch {
        case ex: Exception => {
          today = dpi.getToday
          println("读取文件异常")
          readList.clear()
        }
      }

      if (list.size == 0) {
        println("列表为空")


      }

      else {

        list.foreach(t => {
          if (readList.contains(t)) {
            list = list - t
            println("已移除")

          } else {
            readList.add(t)
          }
        })


        if (list.size == 0) {
          today = dpi.getToday
          dpi.logger.info("------newday--------")

        } else {
          for(t <- list) dpi.logger.info("待传输文件列表： "+t)
          upload(client,list, today)
        }
      }
    }

    spark.close()
  }


  def upload(client:SFTPUtil,list: ListBuffer[String], date: String): Unit = {

        client.login()

    for (filename <- list) {
      client.download(path+date,filename,localpath+filename)

      val df: DataFrame = spark.read.text(localpath+filename)

      println(Calendar.getInstance.getTime + ":文件 " + date + " : " + filename)
      //0x01+0x0300+000+M-JS-SZ+XF+001+20181016021000
      val namedate = filename.substring(31, 39)
      val arr_ds: Dataset[Array[String]] = df.map(t => t.getString(0).split("\\|")).filter(t => t.length == 12)
      val source_ds = arr_ds.map(words => {
        val UserAccount: String = words(0)
        val ProtocolType: String = words(1)
        val SrcIP: String = words(2)
        val DestIP: String = words(3)
        val SrcPort: Int = Integer.parseInt(words(4),16)   //16进制转10进制
        val DescPort: Int =Integer.parseInt(words(5),16)
        val DomainName: String = new String(Base64.decodeBase64(words(6)))
        val URL: String = new String(Base64.decodeBase64(words(7)))
        val referer: String = new String(Base64.decodeBase64(words(8)))
        val UserAgent: String = new String(Base64.decodeBase64(words(9)))
        val Cookie: String = new String(Base64.decodeBase64(words(10)))
        val AccessTime:String = words(11)
        tableData(17, SrcIP, SrcPort, DestIP, DescPort, "", "", DomainName, URL, UserAgent, "", "", referer,
          Cookie, "", "", "", "", "", "", "", ProtocolType,AccessTime, 1, namedate)

      })

      val table: DataFrame = source_ds.withColumn("date",to_date(unix_timestamp($"dt","yyyyMMdd").cast("timestamp"),"yyyyMMdd")).drop("dt")



      //  source_ds.show()
      table.show()
      //  table.write.insertInto("url.dpi")
      FileUtils.deleteQuietly(new File(localpath+filename));

    }
    println("----------list---finish-------------")
   client.logout()

  }




}












































































