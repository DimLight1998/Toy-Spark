package toyspark.utilities

object Extensions {
  implicit class ListWrapper[T](val list: List[T]) {
    def evenlyPartitioned(numPartitions: Int): Iterator[List[T]] = {
      val length = list.length
      if (length % numPartitions == 0) {
        list.grouped(length / numPartitions)
      } else {
        val smallSize          = length / numPartitions
        val bigSize            = smallSize + 1
        val numBigPartitions   = length % numPartitions
        val numSmallPartitions = numPartitions - numBigPartitions
        val partitionSizes = List.tabulate(numBigPartitions)(_ => bigSize) ++ List.tabulate(numSmallPartitions)(_ =>
          smallSize)
        val (ret, _) = partitionSizes.foldLeft((Nil: List[List[T]], list))({
          case ((acc, remain), partitionSize) =>
            val (pre, post) = remain.splitAt(partitionSize)
            (acc :+ pre, post)
        })
        ret.iterator
      }
    }
  }
}
