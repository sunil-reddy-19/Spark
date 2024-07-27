object Reduce_Fun {

  def greatest(arg:List[Int]): Unit = {
    var x = 0
    for (i <- Range(0,arg.size)) {
      if (arg(i) > x) {
        x = arg(i)

      }
    }
    println(x)
  }

  def greatest_reduce_fun(arg: List[Int]): Int = {
    arg.reduceLeft((a, b) => if (a > b) a else b)
  }

  def main(args:Array[String]): Unit = {
    greatest(List(1,25,3,4,5,60,99,19))
    val out = greatest_reduce_fun(List(1,25,3,4,5,60,99,19))
    println("Output from reduceLeft: " + out)
  }

}
