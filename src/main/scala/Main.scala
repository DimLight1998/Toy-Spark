import scala.util.Random

import toyspark._

object Main {
  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

    val counter = Dataset.generate(
      List(4, 4, 4),
      (nodeID, localPartitionID) => (0 until 100).toList.map(i => i + localPartitionID * 100 + nodeID * 1000))
    print(counter.count())

    Communication.close()
  }
}
