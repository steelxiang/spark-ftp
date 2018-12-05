package loaddata

object test {


  def main(args: Array[String]): Unit = {
    val filename="gdpi-20180618.txt.gz"
    val dt=filename.substring(filename.length-15,filename.length-7)

      println(dt)
  }
}
