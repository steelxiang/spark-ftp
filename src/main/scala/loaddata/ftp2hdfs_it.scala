package loaddata


import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.{FtpUtils, SFTPUtil}
import com.jcraft.jsch.SftpException
import loaddata.ftp2hdfs_dpi.dpi
import org.apache.commons.net.util.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionOutputStream, Decompressor}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable._

/**
  * @author xiang
  *         2018/11/15
  */

class ftp2hdfs_it{
  val logger = Logger.getLogger(classOf[ftp2hdfs_it])
}
object ftp2hdfs_it {
  private val it = new ftp2hdfs_it

  var path2 = "/data/yhb/pdc_in/"  ///data/yhb/url_in
  var path1 = "/data/yhb/url_in/"

  var codecClassName = "org.apache.hadoop.io.compress.GzipCodec"

  val tmp="/user/misas_dev/data/tmp/it"
  var fs: FileSystem = null
  var conf = new Configuration

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
  val date = getYester

  def main(args: Array[String]): Unit = {
    conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020")
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    System.setProperty("HADOOP_USER_NAME", "misas_dev")
    fs = FileSystem.get(conf)
    val ftp = new FtpUtils()

    ftp.downloadFile(fs,path1,s"gdpi-$date.txt.gzip",s"$tmp/gdpi-$date.txt.gz")
    ftp.downloadFile(fs,path1,s"lte-$date.txt.gzip",s"$tmp/lte-$date.txt.gz")
    ftp.downloadFile(fs,path1,s"cdpi-$date.txt.gzip",s"$tmp/cdpi-$date.txt.gz")

    ftp.downloadFile(fs,path2,s"lte_cdpi_url_$date.txt.gz",s"$tmp/lte_cdpi_url_$date.txt.gz")
    ftp.downloadFile(fs,path2,s"gdpi_url_$date.txt.gz" ,s"$tmp/gdpi_url_$date.txt.gz")
    ftp.downloadFile(fs,path2,s"3g_cdpi_url_$date.txt.gz",s"$tmp/3g_cdpi_url_$date.txt.gz")

     val strings = getFslist(fs,tmp)


   // upload(path2,s"lte_cdpi_url_$date.txt.gz", 6)
   // upload(path2,s"3g_cdpi_url_$date.txt.gz", 7)   数据不再更新   截止 20171115
   // upload(path2,s"gdpi_url_$date.txt.gzip", 11)    数据不再更新  截止 20171115
   // upload(path1,s"lte-$date.txt.gzip", 8)
   // upload(s"cdpi-$date.txt.gz", 9)
   // upload(path1,s"gdpi-$date.txt.gzip", 10)

    spark.close()
  }


  def upload(filename: String, dataType: Int): Unit = {
    val df = spark.read.text(tmp+filename)
        println(filename)


//    val table = df.map(t => {
//
//      val dataSource: Int = dataType
//      val URL: String =t.getString(0)
//      val Id: String = ""
//      val URL_Time: Int =1
//      val dt: String = date
//      YHtable(dataSource, URL, Id, URL_Time, dt)
//
//    }
//    )

    df.show()
    // table.write.insertInto("dpi")

  }

  println("----------list---finish-------------")


  //获取当天日期
  def getYester: String = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calendar.add(Calendar.DATE,-1)
    val s = dateFormat.format(calendar.getTime)
    dpi.logger.warn("yestoday is : " + s)
    s
  }

  def getFslist(fs:FileSystem,fsPath:String)={
    val fileList: RemoteIterator[LocatedFileStatus] = fs.listLocatedStatus(new Path(fsPath))
    var list: ListBuffer[String] = ListBuffer()
    while(fileList.hasNext){
      val name: String = fileList.next().getPath.getName
      list.append(name)
    }
    list

  }


}












































































