package loaddata



import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import loaddata.ftp2hdfs_dpi.dpi
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
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
  var fs: FileSystem = null
  var conf = new Configuration
  val fsPath = "/user/misas_dev/data/tmp/yh/"

  conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020")
  conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
  System.setProperty("HADOOP_USER_NAME", "misas_dev")
  fs = FileSystem.get(conf)

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

    var list=getList
         upload(list)
    spark.close()
  }


  def upload(list:ListBuffer[String]): Unit ={

    for(filename <-list){
      yh.logger.warn("开始加载 "+filename)

      val df: DataFrame = spark.read.text(fsPath+filename)

      var date=filename.substring(7,15)
      val source_ds: Dataset[Array[String]] = df.map(t => t.getString(0).split("\t")).filter(t=>t.length==2)
      if (filename.startsWith("apk_url")) { date=filename.substring(8,16)
        insertData(source_ds,date,1)}
      if (filename.startsWith("cw_url"))  insertData(source_ds,date,4)
      if (filename.startsWith("dx_url"))  insertData(source_ds,date,2)
      if (filename.startsWith("gw_url"))  insertData(source_ds,date,3)
      if (filename.startsWith("sg_url"))  insertData(source_ds,date,5)
      fs.delete(new Path(fsPath+filename),true)
      yh.logger.warn("删除临时文件： "+filename)

    }
    yh.logger.warn("----------list---finish-------------")
  }

    def insertData(df: Dataset[Array[String]],date:String,datetype:Int)={

      val d=date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8)
        val table: Dataset[YHtable] =df.map(words=> {
        val dataSource: Int = datetype
        val URL: String = words(1)
        val Id: String = words(0)
        val URL_Time: Int = 1
        val dt: String = d
        YHtable(dataSource, URL, Id, URL_Time, dt)
      }
      )

      yh.logger.warn("开始插入数据")
      table.repartition(1).write.insertInto("url.apk")
      yh.logger.warn("插入完毕")

    }

  def getList ={
    val list=ListBuffer[String]()
    val listStatus: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(fsPath),true)

    while (listStatus.hasNext){
            val path: Path = listStatus.next().getPath
          list.append(path.getName)

          }
    list
  }

}












































































