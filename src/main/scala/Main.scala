import scala.util.Random
import Datasets.Dataset
import Communication.{Communication, Context}

object Main {
  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

    val pi = Dataset
      .generate(List(4, 4, 4), (_, _) => (0 until 1000000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
      .coalesced(List(8, 8, 8))
      .filter({ case (x, y) => x * x + y * y < 1 })
      .map(_ => 1)
      .reduce((x, y) => x + y, 0)

    println("The result of Pi is:")
    println(pi / (1000000.0 * 12) * 4)
    println("Bye!")

  }
}
