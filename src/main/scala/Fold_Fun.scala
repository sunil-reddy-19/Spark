object Fold_Fun {

  def fold_fun(args: List[Int]): Int = {
    //fold is same as reduce but here we can initialize the default value
    args.fold(0)((a, b) => a + b)
  }

  def main(args:Array[String]): Unit = {

    println(fold_fun(List(10,20,20,30)))
  }

}
