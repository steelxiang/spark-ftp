package loaddata



import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import loaddata.ftp2hdfs_dpi.dpi
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
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

class ftp2hdfs_yh{

  val logger = Logger.getLogger(classOf[ftp2hdfs_yh])

}
object ftp2hdfs_yh {

  val yh = new ftp2hdfs_yh
  val host="10.4.12.186"
  val userName="yanjiuyuan"
  val password="yanjiuyuan@20180827"
  val port=22
  var path="/home/yanjiuyuan/data"

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ftp-yh")
    .config("spark.shuffle.consolidateFiles", true)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val context = spark.sparkContext
      context.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
       val yestoday=getYester
    val apk=s"apk_url_$yestoday.txt.gz"
    val cw= s"cw_url_$yestoday.txt.gz"
    val dx=s"dx_url_$yestoday.txt.gz"
    val gw=s"gw_url_$yestoday.txt.gz"
    val sg=s"sg_url_$yestoday.txt.gz"
      var list=ListBuffer[String](apk,cw,dx,gw,sg)

         upload(list)
         spark.close()
  }

  def upload(list:ListBuffer[String]): Unit ={

    for(filename <-list){
      yh.logger.warn("开始加载 "+filename)
      val df: DataFrame = spark.read.
        format("com.springml.spark.sftp").
        option("host",host).
        option("username", userName).
        option("password", password).
        option("fileType", "txt").
        option("port", port).
        load(path+"/"+filename)

      var date=filename.substring(7,15)
      val source_ds: Dataset[Array[String]] = df.map(t => t.getString(0).split("\t")).filter(t=>t.length==2)
      if (filename.startsWith("apk_url")) { date=filename.substring(8,16)
        insertData(source_ds,date,1)}
      if (filename.startsWith("cw_url"))  insertData(source_ds,date,4)
      if (filename.startsWith("dx_url"))  insertData(source_ds,date,2)
      if (filename.startsWith("gw_url"))  insertData(source_ds,date,3)
      if (filename.startsWith("sg_url"))  insertData(source_ds,date,5)
      FileUtils.deleteQuietly(new File("/tmp/"+filename))
      yh.logger.warn("删除临时文件： "+filename)
    }
    yh.logger.warn("----------list---finish-------------")
  }

    def insertData(df: Dataset[Array[String]],date:String,datetype:Int)={
        val source_ds: Dataset[YHtable] =df.map(words=> {
        val dataSource: Int = datetype
        val URL: String = words(1)
        val Id: String = words(0)
        val URL_Time: Int = 1
        val dt: String = date
        YHtable(dataSource, URL, Id, URL_Time, dt)
      }
      )
      val table: DataFrame = source_ds.withColumn("date",to_date(unix_timestamp($"dt","yyyyMMdd").cast("timestamp"),"yyyyMMdd")).drop("dt")

     table.show()
      yh.logger.warn("开始插入数据")
      //table.write.insertInto("url.apk")
      yh.logger.warn("插入完毕")

    }


  def getYester: String = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
        calendar.set(Calendar.DATE,-1)
    val s = dateFormat.format(calendar.getTime)
    dpi.logger.warn("yestoday is : " + s)
    s
  }

}












































































