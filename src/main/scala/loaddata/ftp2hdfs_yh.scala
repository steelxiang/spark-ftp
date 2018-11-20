package loaddata



import java.util
import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
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

    val list: ListBuffer[String] = getList
         upload(list)
         spark.close()
  }

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
      val date=filename.substring(7,15)
      val source_ds: Dataset[Array[String]] = df.map(t => t.getString(0).split("\t")).filter(t=>t.length==2)
      if (filename.startsWith("apk_url")) insertData(source_ds,date,1)
      if (filename.startsWith("cw_url"))  insertData(source_ds,date,4)
      if (filename.startsWith("dx_url"))  insertData(source_ds,date,2)
      if (filename.startsWith("gw_url"))  insertData(source_ds,date,3)
      if (filename.startsWith("sg_url"))  insertData(source_ds,date,5)

    }
    println("----------list---finish-------------")
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
     // table.write.insertInto("url.apk")
    }
  //获取目录列表
  def getList()= {

    try {
      val client = new SFTPUtil(userName, password, host, port)
      client.login()
      val filelist: util.Vector[_] = client.listFiles(s"$path")
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



}












































































