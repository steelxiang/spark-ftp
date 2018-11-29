package loaddata

import loaddata.ftp2hdfs_dpi.dpi
import org.apache.commons.net.util.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{to_date, unix_timestamp}

import scala.collection.mutable.ListBuffer

class load_dpi {
  val logger = Logger.getLogger(classOf[load_dpi])
}
object load_dpi{

    val load_dpi = new load_dpi

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("load-dpi")
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

    var fs: FileSystem = null
    var conf = new Configuration
    val fsPath = "/user/misas_dev/data/tmp/yh/"

    conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020")
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    System.setProperty("HADOOP_USER_NAME", "misas_dev")
    fs = FileSystem.get(conf)


    val paths: ListBuffer[Path] = getFslist(fs,"/user/misas_dev/data/dpi/dpilog")

    paths.foreach(t=>{
         println("开始文件夹："+t.toString)
      var list: ListBuffer[Path] = getFslist(fs,t.toString)

      list.foreach(t=>{
       if(t.getName.length!=45)
         list=list-t

      })

      list.foreach(t=>{
        var filename=t.getName
        val namedate = filename.substring(31, 39)
        val dt=namedate.substring(0,4)+"-"+namedate.substring(4,6)+"-"+namedate.substring(6,8)
        load_dpi.logger.warn("开始加载："+filename)
        val frame = spark.read.text(t.toString)
        load_dpi.logger.warn("加载完成："+filename)
        val arr_ds: Dataset[Array[String]] = frame.map(t => t.getString(0).split("\\|")).filter(t => t.length == 12)
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

      load_dpi.logger.warn("插入数据："+filename)
        table.repartition(1).write.insertInto("url.dpi")
        load_dpi.logger.warn("插入完成："+filename)

      })

    })




  }

  def getFslist(fs:FileSystem,fsPath:String)={
    val fileList: RemoteIterator[LocatedFileStatus] = fs.listLocatedStatus(new Path(fsPath))
    var list: ListBuffer[Path] = ListBuffer()
    while(fileList.hasNext){
      val name: Path = fileList.next().getPath
      list.append(name)
    }
    list

  }

  def save2hive(t: Path)={




  }

}
