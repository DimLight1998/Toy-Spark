import toyspark._
import java.util.Date
import scala.util.Random

object TestCase {
  def test(args: Array[String]): Unit = {
    if(args.length > 1) {
      val testid = args(1)

      testid match {
        case "0"           => test0()
        case "1"           => test1()
        case "2"           => test2()
        case "3"           => test3()
        case "4"           => test4()
        case "5"           => test5()
        case "Time"        => testTime()
        case "PrimaryMath" => primaryMath()
        case "CalculatePi" => calculatePi()
        case "SaveHDFS"    => saveHDFS()
        case "LoadHDFS"    => loadHDFS()
      }
    }
  }

  def test0(): Unit = {
    println("Executing test_0.")
    println("Testing filter and count...")
    ////Communication.initialize(args)
    val res = Dataset
      .generate(List(4, 4, 4), (_, _) => List.fill(9)(3) ::: List.fill(1)(4))
      .filter(x => x > 3)
      .count()
    val groundTruth = 4 + 4 + 4
    println("res = " + res)
    println("groundTruth = " + groundTruth)
    println(s"test_0 ${res match{
      case `groundTruth` => "passed"
      case _ => "failed"
    }
    }")
    //Communication.close()
  }

  def test1(): Unit = {
    println("Executing test_1.")
    println("Testing map and collect...")
    ////Communication.initialize(args)
    val res = Dataset
      .generate(List(4, 4, 4), (_, _) => List.fill(10)(5))
      .map(x => x+1)
      .collect(List(0))
    val groundTruth = List.fill(10 * (4 + 4 + 4))(6)
    println("res = " + res)
    println("groundTruth = " + groundTruth)
    println(s"test_1 ${res match{
      case `groundTruth` => "passed"
      case _ => "failed"
    }
    }")
    //Communication.close()
  }

  def test2(): Unit = {
    println("Executing test_2.")
    println("Testing repartition and reduce...")
    ////Communication.initialize(args)
    val res = Dataset
      .generate(List(4, 4, 4), (_, _) => List.fill(10)(5))
      .repartition(List(2, 2, 2))
      .reduce((_, _) => 1, 1)
    val groundTruth = 1
    println("res = " + res)
    println("groundTruth = " + groundTruth)
    println(s"test_2 ${res match{
      case `groundTruth` => "passed"
      case _ => "failed"
    }
    }")
    //Communication.close()
  }

  def test3(): Unit = {
    println("Executing test_3.")
    println("Testing union...")
    //Communication.initialize(args)
    val data1 = Dataset.generate(List(2, 2, 2), (_, _) => List.fill(3)(1))
    val res = Dataset.generate(List(3, 4, 5), (_, _) => List.fill(3)(-1))
      .unionWith(data1)
      .count()
    val groundTruth = 3 * (2 + 2 + 2 + 3 + 4 + 5)
    println("res = " + res)
    println("groundTruth = " + groundTruth)
    println(s"test_3 ${res match{
      case `groundTruth` => "passed"
      case _ => "failed"
    }
    }")
    //Communication.close()
  }

  def test4(): Unit = {
    println("Executing test_4.")
    println("Testing intersection...")
    //Communication.initialize(args)
    val data1 = Dataset.generate(List(2, 2, 2), (_, _) => List(1,2,3,4,5))
    val res = Dataset.generate(List(2, 2, 2), (_, _) => List(3))
      .intersectionWith(data1)
      .collect(List(0))
    val groundTruth = List.fill(2 + 2 + 2)(3)
    println("res = " + res)
    println("groundTruth = " + groundTruth)
    println(s"test_4 ${res match{
      case `groundTruth` => "passed"
      case _ => "failed"
    }
    }")
    //Communication.close()
  }

  def test5(): Unit = {
    println("Executing test_5.")
    println("Testing cartesian and take...")
    //Communication.initialize(args)
    val data1 = Dataset.generate(List(1, 1, 1), (_, _) => List(1,2))
    val res = Dataset.generate(List(1, 1, 1), (_, _) => List(-1,-2))
      .cartesianWith(data1)
      .take(12, List((0, 0)))
    val groundTruth = List((-1,1),(-1,2),(-1,1),(-1,2),(-1,1),(-1,2),(-2,1),(-2,2),(-2,1),(-2,2),(-2,1),(-2,2))
    println("res = " + res)
    println("groundTruth = " + groundTruth)
    println(s"test_5 ${res match{
      case `groundTruth` => "passed"
      case _ => "failed"
    }
    }")
    //Communication.close()
  }

  def testTime(): Unit = {
    println("Testing time consuming...")
    //Communication.initialize(args)
    val startTime = new Date().getTime
    var res = 0.0
    for(i <- 0 until 10) {
      println("In iteration " + i)
      val pi = Dataset
        .generate(List(4, 4, 4), (_, _) => (0 until 1000000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
        .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
        .filter({ case (x, y) => x * x + y * y < 1 })
        .count()
      res = res + pi / (1000000.0 * 12) * 4
    }
    println("The result of Pi estimating by this program is:")
    println(res / 1000)
    val endTime = new Date().getTime
    println("Time consuming is:" + ((endTime - startTime) / 1000) + "s")
    //Communication.close()
  }

  def calculatePi(): Unit = {
    println("Calculating Pi...")
    //Communication.initialize(args)
    val pi = Dataset
      .generate(List(4, 4, 4), (_, _) => (0 until 1000000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
      .filter({ case (x, y) => x * x + y * y < 1 })
      .count()
    println("The result of Pi estimating by this program is:")
    println(pi / (1000000.0 * 12) * 4)
    println("Bye!")
    //Communication.close()
  }

  def primaryMath(): Unit = {
    println("Calculating primary math...")
    //Communication.initialize(args)

    // generate x
    val xs1 = Dataset.generate(List(4, 4, 4), (_, fragID) => {
      println(s"fragment $fragID is generating data for x!")
      List.fill(3)(Random.nextDouble())
    }) // 36
    val xs2 = Dataset.generate(List(2, 2, 2), (_, _) => List.fill(3)(Random.nextDouble())) // 18
    val xs  = xs1.cartesianWith(xs2).map({ case (x1, x2) => x1 + x2 }) // 648
    xs.save()
    println(s"we have ${xs.count()} xs now")

    // generate y
    val ys1 = Dataset.generate(List(4, 6, 6), (_, fragID) => {
      println(s"fragment $fragID is generating data for y!")
      List.fill(5)(Random.nextDouble())
    }) // 80
    val ys2 = Dataset.generate(List(3, 4, 3), (_, _) => List.fill(12)(Random.nextDouble())) // 120
    val ys  = ys1.unionWith(ys2).map(y => y * 2)                                            // 200
    println(s"there is ${ys.count()} ys now")

    // generate points
    val points = xs.cartesianWith(ys) // 648 * 200
    points.save()
    println(s"total number of points: ${points.count()}")

    // filter points
    val pointsInQuarterCircle = points.filter({ case (x, y) => x * x + (y - 2) * (y - 2) < 4 })
    val pointsInSemicircle    = points.filter({ case (x, y) => (x - 1) * (x - 1) + y * y < 1 })
    val pointsInCommon        = pointsInQuarterCircle.intersectionWith(pointsInSemicircle)
    pointsInCommon.save()
    println(pointsInCommon.count())

    // count number and sample some points
    val numPointsInCommon = pointsInCommon.count()
    val numPoints         = points.count()
    val samplesInCommon   = pointsInCommon.collect(Nil).slice(0, 100)

    println(s"estimated area size: ${(numPointsInCommon * 4).toDouble / numPoints.toDouble}")
    println(s"actual area size: ${2 * Math.PI - 3 * Math.atan(2) - 2}")
    println(samplesInCommon)

    //Communication.close()
  }

  def saveHDFS(): Unit = {
    println("Saving 6 random double number to HDFS file...")
    //Communication.initialize(args)
    val data = List(2,4,6,8,10,12)
    val _ = Dataset
      .generate(List(2, 2, 2), (i, j) => List(data(i*2+j)))
      .saveAsSingleFile("/", "TestHDFS.dat")
    println("The written data (in ascending order) is:")
    println(data.sorted)
    //Communication.close()
  }

  def loadHDFS(): Unit = {
    println("Loading the saved random double number from HDFS file...")
    //Communication.initialize(args)
    val res = Dataset.read(List(2,2,2), "/TestHDFS.dat", 0)
        .collect(List(0))
    println("The written data (in ascending order) is:")
    println(res.sorted)
    //Communication.close()
  }
  
}
