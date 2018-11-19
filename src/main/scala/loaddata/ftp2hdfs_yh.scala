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
  * 2018/11/15
  */
case class YHtable(dataSource :Int,
                   URL:String,
                   Id:String,
                   URL_Time:Int,
                   dt	:String)
object ftp2hdfs_yh {
  val host="192.168.5.200"
  val userName="root"
  val password="123456"
  val port="22"
  var path="/root/data/"


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
  def main(args: Array[String]): Unit = {


    var list: ListBuffer[String] =getList
    upload(list)

      }


    spark.close()


  def upload(list:ListBuffer[String]): Unit ={

    for(filename <-list){
      val df: DataFrame = spark.read.
        format("com.springml.spark.sftp").
        option("host",host).
        option("username", userName).
        option("password", password).
        option("fileType", "txt").
        option("port", port).
        load(path+"/"+filename)
       println(filename)
          val date=filename.substring(8,15)
      var source_ds:Dataset[YHtable]=null

      if (filename.startsWith("apk_url")) {
         source_ds = df.map(t => {
          val words: Array[String] = t.getString(0).split("\t")
          val dataSource: Int = 1
          val URL: String = words(1)
          val Id: String = words(0)
          val URL_Time: Int = 1
          val dt: String = date
          YHtable(dataSource, URL, Id, URL_Time, dt)

        }
        )
        source_ds
      }
      else if (filename.startsWith("cw_url")) {
        source_ds =df.map(t=>{
          val words: Array[String] = t.getString(0).split("\t")
          val dataSource:Int=4
          val URL:String=words(1)
          val Id:String=words(0)
          val URL_Time:Int=1
          val dt	:String=date
          YHtable(dataSource,URL,Id,URL_Time,dt)

        }
        )
        source_ds
      }
      else if (filename.startsWith("dx_url")) {
        source_ds =df.map(t=>{
          val words: Array[String] = t.getString(0).split("\t")
          val dataSource:Int=2
          val URL:String=words(1)
          val Id:String=words(0)
          val URL_Time:Int=1
          val dt	:String=date
          YHtable(dataSource,URL,Id,URL_Time,dt)

        }
        )
        source_ds
      }
      else if (filename.startsWith("gw_url")) {
        source_ds =df.map(t=>{
          val words: Array[String] = t.getString(0).split("\t")
          val dataSource:Int=3
          val URL:String=words(1)
          val Id:String=words(0)
          val URL_Time:Int=1
          val dt	:String=date
          YHtable(dataSource,URL,Id,URL_Time,dt)

        }
        )
        source_ds
      }
      else if (filename.startsWith("sg_url")) {
        source_ds =df.map(t=>{
          val words: Array[String] = t.getString(0).split("\t")
          val dataSource:Int=5
          val URL:String=words(1)
          val Id:String=words(0)
          val URL_Time:Int=1
          val dt	:String=date
          YHtable(dataSource,URL,Id,URL_Time,dt)

        }
        )
        source_ds
      }else{
        null
      }







      val table = source_ds.withColumn("date",date_format(unix_timestamp($"dt","yyyyMMdd").cast("timestamp"),"yyyyMMdd")).drop("dt")

      table.show()
      // table.write.partitionBy("date").insertInto("dpi")

    }
    println("----------list---finish-------------")



  }



  //获取目录列表
  def getList()= {


      val client = new SFTPUtil(userName, password, host, 22)
      client.login()
      val filelist: util.Vector[_] = client.listFiles(s"$path")
      client.logout()
      val list:ListBuffer[String]= ListBuffer()
      for (i <- 0 until filelist.size()) {
        val str = filelist.get(i).toString.split("\\s+").last
        if (!".".equals(str) && !"..".equals(str)) {
          list.append(str)

        }
      }

      list


  }



}












































































