object Scan_fun {

  def cum_sum(arg:List[Int]) = {
    arg.scan(0)((a,b) => a+b)
  }

  def main(args:Array[String]): Unit = {
    println(cum_sum(List(1,2,3,4,5)))

    val x = Seq(1,2,3,4,5).scan(0)(_+_)

    println(x)
  }

}
