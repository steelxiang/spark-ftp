package loaddata

object test {


  def main(args: Array[String]): Unit = {
    val namedate="20181212"
    val dt=namedate.substring(0,4)+"-"+namedate.substring(4,6)+"-"+namedate.substring(6,8)

      println(dt)
  }
}
