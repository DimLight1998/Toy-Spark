import org.apache.commons.lang3.SerializationUtils
import scala.util.Random
import toyspark._

object Main {
  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

    //println(HDFSUtil.listAll("/"))
    //val data = SerializationUtils.serialize((0 until 1200000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
    //println(HDFSUtil.createNewHDFSFile("/CalculatePi.data", data))

    // val pi = Dataset
    //   //.generate(List(4, 4, 4), (_, _) => (0 until 1000000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
    //   .read(List(4, 4, 4), "/CalculatePi.data", (0.0, 0.0))
    //   .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
    //   .coalesced(List(2, 2, 2))
    //   .filter({ case (x, y) => x * x + y * y < 1 })
    //   .count()

    //   //.saveAsSequenceFile("/InCircleDir", "InCircleData")

    //   //.take(10, List((0.0, 0.0)))

    //   //.map(_ => 1)
    //   //.reduce((x, y) => x + y, 0)

    // println("The result of Pi is:")
    // println(pi / 1200000.0 * 4)
    // //println(pi)
    // println("Bye!")

    val pi = Dataset
      .generate(List(4, 4, 4), (_, _) => (0 until 1000000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
      .coalesced(List(8, 8, 8))
      .filter({ case (x, y) => x * x + y * y < 1 })
      .map(_ => 1)
      .reduce((x, y) => x + y, 0)
    //	 .collect(Nil)
    println(s"The result of Pi is ${pi / (1000000.0 * 12) * 4}")

  }
}
