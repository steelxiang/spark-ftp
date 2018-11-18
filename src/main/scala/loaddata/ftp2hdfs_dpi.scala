package loaddata


import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import Utils.SFTPUtil
import com.jcraft.jsch.SftpException
import org.apache.commons.net.util.Base64
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


import scala.collection.mutable._
/**
  * @author xiang
  * 2018/11/15
  */


case class tableData(commandId	:String ,
                    src_ip :String ,
                    src_port :String ,
                    dest_ip :String ,
                    dest_port :String ,
                    request_time :String ,
                    response_time :String ,
                    domain :String ,
                    url :String ,
                    user_agent :String ,
                    content_type :String ,
                    content_size	:String ,
                    refer	 :String ,
                    cookie :String ,
                    method :String ,
                    status :String ,
                    proxy_type :String ,
                    proxy_ip :String ,
                    proxy_port :String ,
                    link :String ,
                    view :String ,
                    transport_protocol:String ,
                    dataSource:	Int,
                    dt:	String )
object ftp2hdfs_dpi {
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
      val flag=true
      val readList= new util.ArrayList[String]()  //存放当天已读过的文件列表
      var today= getToday

    while(flag){
       println("hahahhha")
      var list: ListBuffer[String] = ListBuffer[String]()
    try{

      list=getList(today)
    }  catch {
      case ex:Exception =>{today=getToday
        println("11111111111111111111")
                  readList.clear()
      }
    }

     if(list.size==0){
       println("newday")
       today=getToday
       //readList.clear()
     }

     else {

       list.foreach(t=>{
         if (readList.contains(t)) {
            list = list-t
           println("已移除")

         }else{
           readList.add(t)
         }
       })




            if (list.size == 0) {
              today = getToday
              println("newday")

            } else {

              upload(list, today)
            }
          }
    }





    spark.close()
  }

  def upload(list:ListBuffer[String],date:String): Unit ={

    for(filename <-list){
    val df: DataFrame = spark.read.
      format("com.springml.spark.sftp").
      option("host",host).
      option("username", userName).
      option("password", password).
      option("fileType", "txt").
      option("port", port).
      load(path+date+"/"+filename)
      println(date+" : "+filename)

      //0x01+0x0300+000+M-JS-SZ+XF+001+20181016021000
      val namedate=filename.substring(31,39)
      val source_ds= df.map(t => {
        val words: Array[String] = t.getString(0).split("\\|")
           val UserAccount :String=words(0)
           val  ProtocolType :String =words(1)
           val  SrcIP :String =words(2)
           val   DestIP :String =words(3)
           val  SrcPort :String =words(4)
           val  DescPort :String =words(5)
           val   DomainName :String=new String(Base64.decodeBase64(words(6)))
           val  URL :String =new String(Base64.decodeBase64(words(7)))
           val  referer :String= new String(Base64.decodeBase64(words(8)))
           val  UserAgent :String =new String(Base64.decodeBase64(words(9)))
            val  Cookie :String =new String(Base64.decodeBase64(words(10)))
            val  AccessTime :String=words(11)
          tableData("17",SrcIP,SrcPort,DestIP,DescPort,"","",DomainName,URL,UserAgent,"","",referer,
          Cookie,"","","","","","","",ProtocolType,1,namedate)


      })

      val table: DataFrame = source_ds.withColumn("date",date_format(unix_timestamp($"dt","yyyyMMdd").cast("timestamp"),"yyyyMMdd")).drop("dt")

       table.show()
     // table.write.partitionBy("date").insertInto("dpi")

    }
   println("----------list---finish-------------")



  }



  //获取目录列表
  def getList(date:String)= {

    try {
      val client = new SFTPUtil(userName, password, host, 22)
      client.login()
      val filelist: util.Vector[_] = client.listFiles(s"$path$date")
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
    catch {
      case ex: SftpException =>throw new Exception("listFiles exception")
    }

  }

//获取当天日期
  def getToday: String = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val s = dateFormat.format(calendar.getTime)
    println("today is : "+s)
    s
  }
}












































































