object loops {

  def whileloop(): Unit = {

    var i=5

    while (i >= 0)
      {
        println("whileloop: "+i)
        i -= 1
      }

  }

  def dowhileloop(): Unit = {

    var i=0

    do {
      println("do while loop: " + i)
      i += 1
    }
    while (i <= 5)
  }

  def forloop(): Unit = {

    for (i <- 1 to 5)
      {
        println("for loop: "+i)
      }

    for (i <- Range(0,7))
      {
        println("for loop with range: " + i)
      }

  }

  def main(args:Array[String]): Unit = {

    whileloop()
    dowhileloop()
    forloop()
  }

}
