object Anonymus_Fun {

  def main(args:Array[String]): Unit = {

    var strCont = (_:String) + (_:String)
    var strCont1 = (str1: String, str2: String) => str1 + str2

    println(strCont("Sunil","Reddy"))
    println(strCont1("Sunil","Reddy"))


  }
}
