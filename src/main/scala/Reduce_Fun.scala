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

  def max_num(arg:List[Int]): Int = {
    arg.reduce((a,b) => a max b)
  }

  def main(args:Array[String]): Unit = {
    greatest(List(1,25,3,4,5,60,99,19))
    val out = greatest_reduce_fun(List(1,25,3,4,5,60,99,19))
    println("Output from reduceLeft: " + out)
    println(max_num(List(1,23,45,6,143,67,99)))

    println(List(1,20000,-34).reduce((a,b)=>a min b))

    val tup = (1,"sunil",true)

    println(tup)
    println(tup._2)
    val abc = tup.toString()
    println(abc.slice(3,8))
  }

}
