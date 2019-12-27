import scala.util.Random
import toyspark._

object Main {
  def xxast(): Unit = {
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
      List.fill(10)(Random.nextDouble())
    }) // 160
    val ys2 = Dataset.generate(List(3, 4, 3), (_, _) => List.fill(50)(Random.nextDouble())) // 500
    val ys  = ys1.unionWith(ys2).map(y => y * 2)                                            // 660
    println(s"there is ${ys.count()} ys now")

    // generate points
    val points = xs.cartesianWith(ys) // 648 * 660
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
  }

  def flatMapTest(): Unit = {
    val a = Dataset.generate(List(2, 3, 3), (nid, pid) => List(s"[$nid $pid]", s"($nid $pid)", s"{$nid, $pid}"))
    val b = a.flatMap(x => List(s"<$x>", s"<<$x>>", s"<<<$x>>>"))
    println(b.collect(Nil))
  }

  def distinctTest(): Unit = {
    val a = Dataset.generate(List(4, 4, 4), (_, _) => List.fill(1000)(Random.nextInt(100)))
    println(a.distinct().collect(Nil))
  }

  def groupByKeyTest(): Unit = {
    def gen(): (Int, String) = {
      val a = Random.nextInt(10)
      val b = a.toString + Random.nextInt(100)
      (a, b)
    }
    val a = Dataset
      .generate(List(4, 4, 4), (_, _) => List.fill(20)(gen()))
      .groupByKey()
    println(a.collect(Nil))
  }

  def reduceByKeyTest(): Unit = {
    def gen(): (Int, String) = {
      val a = Random.nextInt(10)
      val b = a.toString + "<" + Random.nextInt(100) + ">"
      (a, b)
    }
    val a = Dataset
      .generate(List(4, 4, 4), (_, _) => List.fill(20)(gen()))
      .reduceByKey((a: Any, b: Any) => a.asInstanceOf[String] + b.asInstanceOf[String])
    println(a.collect(Nil))
  }

  def joinTest(): Unit = {
    def gen(): (Int, List[String]) = {
      val a = Random.nextInt(20)
      val b = a.toString + "<" + Random.nextInt(100) + ">"
      val c = a.toString + "<" + Random.nextInt(100) + ">"
      val d = a.toString + "<" + Random.nextInt(100) + ">"
      (a, List(b, c, d))
    }

    val a = Dataset.generate(List(4, 4, 4), (_, _) => List.fill(20)(gen()))
    val b = Dataset.generate(List(4, 4, 4), (_, _) => List.fill(20)(gen()))
    println(a.joinWith(b).count())
  }

  def pageRank(): Unit = {
    def randomSourceURL()      = Random.nextPrintableChar() + Random.nextInt(10)
    def randomDestinationURL() = Random.nextPrintableChar() + Random.nextInt(10)

    val iters = 10
    val links = Dataset
      .generate(List(4, 4, 4), (_, _) => List.fill(1000)(randomSourceURL(), randomDestinationURL()))
      .distinct()
      .groupByKey()
    links.save()
    var ranks = links.map({ case (k, _) => (k, 1.0) })

    for (_ <- 1 to iters) {
      val contribs = links
        .joinWith(ranks)
        .flatMap({
          case (_, (urls: List[Any], rank: Double)) =>
            val size = urls.size
            urls.map(url => (url, rank / size))
        })
      ranks = contribs
        .reduceByKey((x, y) => x.asInstanceOf[Double] + y.asInstanceOf[Double])
        .map({ case (k, v: Double) => (k, 0.15 + 0.85 * v) })
    }

    val output = ranks.collect(Nil)
    output.foreach(tup => println(s"${tup._1} has rank: ${tup._2}"))
  }

  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

//    pageRank()
    xxast()

    Communication.close()
  }
}
