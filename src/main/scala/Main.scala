import scala.util.Random
import Datasets.Dataset
import Communication.{Communication, Context}

object Main {
  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

//    // todo debug only
//    Context.setNodeId(0)
//    val pi = Dataset
//      .generate(List(4, 4, 4), (_, _) => (0 until 10000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
//      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
//      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
//      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
//      .coalesced(List(3, 3, 3))
//      .filter({ case (x, y) => x * x + y * y < 1 })
//      .filter({ case (x, y) => x * x + y * y < 1 })
//      .filter({ case (x, y) => x * x + y * y < 1 })
//      .filter({ case (x, y) => x * x + y * y < 1 })
//      .coalesced(List(2, 3, 3))
//      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
//      .collect()
//      .length

    val pi = Dataset
      .generate(List(4, 4, 4), (_, _) => (0 until 1000000).map(_ => (Random.nextDouble(), Random.nextDouble())).toList)
      .map({ case (x, y) => (x * 2 - 1, y * 2 - 1) })
//      .coalesced(20)
      .filter({ case (x, y) => x * x + y * y < 1 })
      .collect()
      .length

    println("The result of Pi is:")
    println(pi / (1000000.0 * 12) * 4)
    println("Bye!")
  }
}
