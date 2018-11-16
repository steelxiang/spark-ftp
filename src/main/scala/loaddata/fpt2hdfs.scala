package loaddata

import com.springml.sftp.client.SFTPClient
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiang
  * 2018/11/15
  */
object fpt2hdfs {


  val spark: SparkSession = SparkSession
    .builder()
    .appName("yangmaodang")
    .config("spark.shuffle.consolidateFiles", true)
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  //读取数据  192.168.5.202:22
  val context = spark.sparkContext
  context.setLogLevel("WARN")
  val sqlcontext = spark.sqlContext
  def main(args: Array[String]): Unit = {




//    val df: DataFrame = spark.read.
//      format("com.springml.spark.sftp").
//      option("host", "180.100.230.178").
//      option("username", "chenzs").
//      option("password", "Jiangsumisas").
//      option("fileType", "txt").
//      option("port", "14333").
//      load("/home/opt/data/log/20181116/0x01+0x0300+000+M-JS-SZ+XF+002+20181116071500")
//      df.show()
//    val df: DataFrame = spark.read.
//      format("com.springml.spark.sftp").
//      option("host", "192.168.5.200").
//      option("username", "root").
//      option("password", "123456").
//      option("fileType", "txt").
//      option("port", "22").
//      load("/root/001.txt")
//      df.show()

//    val df: DataFrame = spark.read.
//          format("com.springml.spark.sftp").
//          option("host", "10.4.12.186").
//          option("username", "yanjiuyuan").
//          option("password", "yanjiuyuan@20180827").
//          option("fileType", "txt").
//          option("port", "22").
//          load("/home/yanjiuyuan/data/dx_url_20181115.txt.gz")
//          df.show()




   // val dataSource="ftp://yanjy_lch:UxGsD1a#,kA@10.4.41.99:21/data/yhb/pdc_in/"
   // val dataSource="ftp://misas_dev:misas_dev1@172.31.20.152:22/home/misas_dev/nohup.out"


 //  context.addFile(dataSource)
   // context.addFile(dataSource,true)
  //  val str: String = SparkFiles.get("nohup.out")

                  //  val str1 = SparkFiles.getRootDirectory()
  //  println(str1)
   // frame.foreach(t=>println(t))
    val frame = spark.read.textFile("ftp://yanjy_lch:UxGsD1a#,kA@10.4.41.99:21/data/yhb/pdc_in/")
   // frame.show()
   // df.show()
    frame.show()
    spark.close()

  }

  def getList:String={

    //new SFTPClient()


    ""
  }

}












































































