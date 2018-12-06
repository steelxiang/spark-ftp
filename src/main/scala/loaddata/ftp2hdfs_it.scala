package loaddata


import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.{FtpUtils, SFTPUtil}
import com.jcraft.jsch.SftpException
import loaddata.ftp2hdfs_dpi.dpi
import org.apache.commons.io.FileUtils
import org.apache.commons.net.util.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionOutputStream, Decompressor}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
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
  var localdir = "/tmp/"

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
 // context.setLogLevel("WARN")
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

     var filelist: ListBuffer[String] = getFslist(fs,tmp)
       val saveList =getsaveList

    filelist.foreach(t=>{
      if(saveList.contains(t)){
        filelist=filelist-t
        it.logger.info("文件已上传过："+t)
      }
    })

//    6  手机3/4G数据，lte_cdpi_url
//    7  手机3/4G数据，3g_cdpi_url
//    8  手机4G数据，lte
//    9  C网数据，cdpi
//    10 G网数据，gdpi
//    11 G网数据, gdpi_url

    if(filelist.length>0) {

      filelist.foreach(t => {
        if (t.startsWith("3g_cdpi_url")) save2hive(t, 7) //数据不再更新   截止 20171115
        if (t.startsWith("gdpi_url")) save2hive(t, 11) //数据不再更新   截止 20171115
        if (t.startsWith("lte_cdpi_url")) save2hive(t, 6) //
        if (t.startsWith("gdpi-")) save2hive(t, 10)
        if (t.startsWith("lte-")) save2hive(t, 8)
        if (t.startsWith("cdpi-")) save2hive(t, 9)

      })
    }else{
      println("列表为空")
    }

    fs.close()
    spark.close()
  }


  def save2hive(filename: String, dataType: Int): Unit = {
    val df = spark.read.text(tmp+"/"+filename)

        println(filename)
    val namedate=filename.substring(filename.length-15,filename.length-7)
    val d=namedate.substring(0,4)+"-"+namedate.substring(4,6)+"-"+namedate.substring(6,8)
    val table = df.map(t => {
            val line: Array[String] = t.getString(0).split("\t")
      val dataSource: Int = dataType
      var URL: String =line(0)
      var URL_Time: Int =1
      if(filename.startsWith("lte_cdpi_url") || filename.startsWith("3g_cdpi_url")) {
        URL=line(1)
      }else{
        URL_Time=line(1).toInt
      }
      val Id: String = ""
      val dt: String = d
      YHtable(dataSource, URL, Id, URL_Time, dt)

    }
    )
    table.repartition(1).write.insertInto("url.apk")
     save2list(filename)
    fs.delete(new Path(tmp+"/"+filename),true)
    it.logger.warn("删除临时fs文件 "+filename)

  }

  println("----------list---finish-------------")


  //获取当天日期
  def getYester: String = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calendar.add(Calendar.DATE,-1)
    val s = dateFormat.format(calendar.getTime)
    it.logger.warn("yestoday is : " + s)
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

  def save2list(filename:String): Unit ={

    FileUtils.writeLines(new File("/home/misas_dev/data2hdfs/tmp/it/filelist.txt"),util.Arrays.asList(filename))
  }

  def getsaveList ={

    val list: util.List[String] = FileUtils.readLines(new File("/home/misas_dev/data2hdfs/tmp/it/filelist.txt"))
    list
  }


}












































































