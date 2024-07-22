object sum_odd_num extends App{
  def sumOfOddNum(x:List[Int]): Unit = {
    var out = 0
    for (i <- x) {

      if (i % 2 != 0) {
        out += i
      }
    }
    println(out)
  }
  val lst = List(1,2,3,4,5)
  sumOfOddNum(lst)
}
