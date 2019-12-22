import scala.util.Random

import toyspark._

object Main {
  def main(args: Array[String]): Unit = {
    Communication.initialize(args)

    val lhs = Dataset.generate(
      List(4, 4, 4),
      (nodeID, localPartitionID) => (0 until 1000).toList.map(i => i + localPartitionID * 10000 + nodeID * 100000))

    println(lhs.count())
    val rhs = Dataset.generate(
      List(2, 3, 3),
      (nodeID, localPartitionID) => (0 until 2000).toList.map(i => i + localPartitionID * 10000 + nodeID * 100000))
    val mappedRHS = rhs.map(x => -3 * x).repartition(List(4, 2, 2)).filter(x => x % 2 == 0)
    val joint     = lhs.unionWith(mappedRHS)

    println(joint.collect(Nil).mkString(", "))
    println(joint.count())

    Communication.close()
  }
}
