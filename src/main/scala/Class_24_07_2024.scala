object Class_24_07_2024 {

    def maxInt(arr: Array[Int]): Int = {
      arr.reduceLeft((x,y) => if (x > y) x else y)

    }

  def main(args: Array[String]): Unit = {

    val x = maxInt(Array(1, 2, 30, 4, 5))
    println(x)

    val realNames = List("peter parker", "clarke kent", "robert ", "bruce")

    val realNamesLen = realNames.foldLeft(0)((x,y) => x+y.length)
    println(realNamesLen)

    val studentMarks = Map(
      "student1" -> List(70, 75, 80, 90),
      "student2" -> List(90, 95, 80, 70),
      "student3" -> List(80, 90, 75, 100)
    )
    println(studentMarks.values)

    val out = studentMarks.map { x =>
      val student = x._1
      val marks = x._2
      val total = marks.sum
      val avg = total.toDouble/marks.size
      (student,avg)
    }
    println(out)


    val totalMarks = studentMarks.flatMap(x => x._2).sum
    val totalAvg = studentMarks.flatMap(x => x._2).sum/studentMarks.count(_ => true)

    println("Total Marks of all Students: " + totalMarks)
    println("Avg of all students: " + totalAvg)


    val tup = (1,2,3,4,5)
    println(tup._1)

    val mapVar = Map("id" -> List(1,2,3),
    "Name" -> List("a","b","c"))

    println(mapVar)
    println(mapVar("id"))



  }

}
