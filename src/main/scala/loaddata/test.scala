package loaddata

import java.util
import java.util.ArrayList

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

class test {

  val logger = Logger.getLogger(classOf[test])


}
object test{

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

    val listBuffer = getList()

    listBuffer.foreach(t=>println(t))
    val value: Dataset[String] = spark.read.textFile("/user/misas_dev/data/YH/apk_url/apk_url_20181112.txt.gz")
    value.show()

  }




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