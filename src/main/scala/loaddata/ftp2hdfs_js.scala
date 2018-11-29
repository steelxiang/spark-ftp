package loaddata


import java.io.{BufferedReader, File, FileReader, IOException}
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.{JS_hdfs, SFTPUtil}
import com.jcraft.jsch.SftpException
import loaddata.ftp2hdfs_dpi.{dpi, localpath}
import org.apache.commons.io.FileUtils
import org.apache.commons.net.util.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable._
import scala.io.Source

/**
  * @author xiang
  *         2018/11/15
  */


class ftp2hdfs_js{

  val logger = Logger.getLogger(classOf[ftp2hdfs_js])

}
object ftp2hdfs_js {

  val js = new ftp2hdfs_js
  val host = "172.31.20.172"
  val userName = "root"
  val password = "Hg!35#89s"
  val port = 22
  var ftpPath = "/data1/Data/ApkUrlData/apkData_JS"
  val localpath="/home/misas_dev/data2hdfs/tmp/js/filelist"
  val locatmp="/home/misas_dev/data2hdfs/tmp/js/"
  val fsPath="/user/misas_dev/data/tmp/js/"

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ftp-js")
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

    val conf = new Configuration
    conf.set("fs.defaultFS", "hdfs://172.31.20.176:8020")
    System.setProperty("HADOOP_USER_NAME", "misas_dev")
    val fs: FileSystem = FileSystem.get(conf)

    js.logger.warn("读取列表")
    var readList = readsaveList()
       var list = getList
    //判断文件是否已上传
    list.foreach(t=>{
      if (readList.contains(t)) {
        list = list - t


      } else {
        readList.add(t)
      }

    })


    dpi.logger.warn("待上传列表：")
    list.foreach(t=>println(t))
    //下载到本地

   down2local(list)


    list.foreach(t=>{
      JS_hdfs.append(fs,locatmp+t,fsPath+t.substring(3,11))
      save2list(t)
    })

    val fslist = getFslist(fs,fsPath)


    save2hive(fs,fslist)



    spark.close()
  }

  def save2hive(fs:FileSystem,list: ListBuffer[String]): Unit = {

    var result:DataFrame=null
    for (filename <- list) {
      js.logger.warn("开始加载： "+filename)

      val dt=filename.substring(0,4)+"-"+filename.substring(4,6)+"-"+filename.substring(6,8)
      val df: DataFrame = spark.read.text(fsPath + filename)

          val table = df.map(t => {

            val dataSource: Int = 12
            val URL: String =t.getString(0)
            val Id: String = ""
            val URL_Time: Int =1
            val dt: String = filename
            YHtable(dataSource, URL, Id, URL_Time, dt)

          }
          )



      // table.show()
      js.logger.warn("开始插入： "+filename)
      table.repartition(1).write.insertInto("url.apk")
      js.logger.warn("插入完成： "+filename)

      fs.delete(new Path(fsPath + filename),true)
      js.logger.warn("删除临时文件： "+filename)

    }
    js.logger.warn("----------list---finish-------------")


  }


  //获取目录列表
  def getList = {

    try {
      val client = new SFTPUtil(userName, password, host, port)
      client.login()
      val filelist: util.Vector[_] = client.listFiles(ftpPath)
      var list: ListBuffer[String] = ListBuffer()
      for (i <- 0 until filelist.size()) {
        val str = filelist.get(i).toString.split("\\s+").last
        if (str.endsWith(".txt")) {
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



//读取已经上传文件那列表
  def readsaveList()={
    val f=new File(localpath)
    if(!f.exists()) f.createNewFile()
    val list: util.List[String] = FileUtils.readLines(f)
    list
  }

  def save2list(filename:String)={
    val f=new File(localpath)
    if(!f.exists()) f.createNewFile()
    FileUtils.writeLines(f,util.Arrays.asList(filename))
  }


  @throws[IOException]
  def append(fs:FileSystem ,localpath: String, FSpath: String): Unit = {
    dpi.logger.warn("上传文件："+localpath)
    val path = new Path(FSpath)
    var append: FSDataOutputStream  = null
    if (fs.exists(path)) {
      append = fs.append(path)
    }
    else {
      append = fs.create(path)

    }



    Source.fromFile(new File(localpath)).getLines.foreach { line =>

      append.writeBytes(line+"\n")
    }

    //删除本地文件
    FileUtils.deleteQuietly(new File(localpath))
    dpi.logger.warn("删除临时文件："+localpath)
      append.flush()
      append.close()
    }


  def down2local(list:ListBuffer[String]): Unit ={
    val client = new SFTPUtil(userName, password, host, port)
    client.login()
    list.foreach(filename=>{
      dpi.logger.warn("开始下载文件： "+filename)
      client.download(ftpPath,filename,locatmp+filename)
    })
    client.logout()


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












































































