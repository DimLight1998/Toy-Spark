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

  implicit class TupleListWrapper[T, U](val list: List[(T, U)]) {
    def groupByKey(): List[(T, List[U])] =
      list.groupBy({ case (k, v) => k }).map({ case (k, v) => (k, v.map({ case (kk, vv) => vv })) }).toList
    def reduceByKey(reducer: (U, U) => U): List[(T, U)] =
      list.groupByKey().map({ case (k, v) => (k, v.reduce(reducer)) })
    def getKeyHashes: List[Int] = {
      list.map({ case (k, v) => k.hashCode() })
    }
  }

  implicit class ListTupleListWrapper[T, U](val list: List[(T, U)]) {
    def joinWith[K](other: List[(T, K)]): List[(T, (U, K))] = {
      def filterAux[M](list: List[(T, M)], key: T): List[M] = {
        list.filter({case (k, _) => k == key}).map({case (_, v) => v})
      }
      val ks1 = list.map({ case (k, _)  => k })
      val ks2 = other.map({ case (k, _) => k })
      val commonKeys = ks1.intersect(ks2).distinct
      commonKeys.flatMap(k => {
        val elems1 = filterAux(list, k)
        val elems2 = filterAux(other, k)
        for(e1 <- elems1; e2 <- elems2) yield (k, (e1, e2))
      })
    }
  }
}
