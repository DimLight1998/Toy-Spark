import scala.util.Random

import toyspark._

object Main {
  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

    val lhs = Dataset.generate(
      List(4, 4, 4),
      (nodeID, localPartitionID) => (0 until 10).toList.map(i => i + localPartitionID * 100 + nodeID * 1000))

    print(lhs.count())
    val rhs = Dataset.generate(
      List(2, 3, 3),
      (nodeID, localPartitionID) => (0 until 20).toList.map(i => i + localPartitionID * 100 + nodeID * 1000))
    val mappedRHS = rhs.map(x => -3 * x).filter(x => x % 2 == 0)
    val joint     = lhs.unionWith(mappedRHS)

    print(joint.collect(Nil).mkString(", "))
    print(joint.count())

    Communication.close()
  }
}
