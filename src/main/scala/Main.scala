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
  }

  def flatMapTest(): Unit = {
    val a = Dataset.generate(List(2, 3, 3), (nid, pid) => List(s"[$nid $pid]", s"($nid $pid)", s"{$nid, $pid}"))
    val b = a.flatMap(x => List(s"<$x>", s"<<$x>>", s"<<<$x>>>"))
    println(b.collect(Nil))
  }

  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

    val a = Dataset.generate(List(4, 4, 4), (_, _) => List.fill(1000)(Random.nextInt(100)))
    println(a.distinct().collect(Nil))

    Communication.close()
  }
}
