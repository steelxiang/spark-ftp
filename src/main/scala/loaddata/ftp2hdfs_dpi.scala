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

}
object ftp2hdfs_dpi {

   val dpi = new ftp2hdfs_dpi
  val host = "180.100.230.178"
  val userName = "chenzs"
  val password = "Jiangsumisas"
  val port = 14333
  var path = "/home/opt/data/log/"
  val localpath="/home/misas_dev/data2hdfs/tmp/dpi/"

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
  context.setLogLevel("WARN")
  val sqlcontext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    val flag = true
    var today = getToday
    while (flag) {

      var readList:util.List[String] =readsaveList(today) //存放当天已读过的文件列表

      dpi.logger.warn("读取列表")
      var list: ListBuffer[String] = ListBuffer[String]()
      try {

        list = getList(today)
      } catch {
        case ex: Exception => {
          today = getToday
          dpi.logger.warn("读取文件异常")
          Thread.sleep(6000)

        }
      }

      if (list.size == 0) {
        dpi.logger.warn("列表为空")
        Thread.sleep(6000)
      }

      else {

        list.foreach(t => {
          if (readList.contains(t)) {
            list = list - t
          //  dpi.logger.warn("已移除 "+t)

          } else {
            readList.add(t)
          }
        })

        if (list.size == 0) {
          today = getToday
          dpi.logger.warn("newday")

        } else {
          try{

            upload(list, today)
          }catch {
            case ex: Exception => {
              dpi.logger.warn("读取文件异常")
              today = getToday
              Thread.sleep(6000)
            }
          }
        }
      }
    }

    spark.close()
  }

  def upload(list: ListBuffer[String], date: String): Unit = {

    for (filename <- list) {
      dpi.logger.warn("开始加载： "+filename)
      val df: DataFrame = spark.read.
        format("com.springml.spark.sftp").
        option("host", host).
        option("username", userName).
        option("password", password).
        option("fileType", "txt").
        option("port", port).
        load(path + date + "/" + filename)
    //  println(Calendar.getInstance.getTime + ":文件 " + date + " : " + filename)

      //0x01+0x0300+000+M-JS-SZ+XF+001+20181016021000
      val namedate = filename.substring(31, 39).format()
      val dt=namedate.substring(0,4)+"-"+namedate.substring(4,6)+"-"+namedate.substring(6,8)
      val arr_ds: Dataset[Array[String]] = df.map(t => t.getString(0).split("\\|")).filter(t => t.length == 12)
      val table = arr_ds.map(words => {
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
          Cookie, "", "", "", "", "", "", "", ProtocolType,AccessTime, 1, dt)

      })





     // table.show()
      dpi.logger.warn("开始插入： "+filename)
      table.repartition(1).write.insertInto("url.dpi")
      dpi.logger.warn("插入完成： "+filename)
      FileUtils.deleteQuietly(new File("/tmp/"+filename))
      dpi.logger.warn("删除临时文件： "+filename)
      writeList(filename,date)
    }
     dpi.logger.warn("----------list---finish-------------")


  }


  //获取目录列表
  def getList(date: String) = {

    try {
      val client = new SFTPUtil(userName, password, host, port)
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
    dpi.logger.warn("today is : " + s)
    s
  }



  def readsaveList(date:String)={
    val f=new File(localpath+date)
    if(!f.exists()) f.createNewFile()
    val list: util.List[String] = FileUtils.readLines(f)
    list
  }



  def writeList(name:String,date:String)={


    FileUtils.writeStringToFile(new File(localpath+date),name+"\n",true)





  }




}












































































