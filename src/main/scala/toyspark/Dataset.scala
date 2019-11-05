package toyspark

abstract class Dataset[T] {
  def map[U](mapper: T => U): MappedDataset[T, U]           = MappedDataset(this, mapper)
  def filter(pred: T => Boolean): FilteredDataset[T]        = FilteredDataset(this, pred)
  def coalesced(partitions: List[Int]): CoalescedDataset[T] = CoalescedDataset(this, partitions)
  def reduce(reducer: (T, T) => T, workerRet: T): T         = ReduceAction(this, reducer, workerRet).perform()
  def collect(workerRet: List[T]): List[T]                  = CollectAction(this, workerRet).perform()
}

object Dataset {
  def generate[T](partitions: List[Int], generator: (Int, Int) => List[T]): GeneratedDataset[T] =
    GeneratedDataset(partitions, generator)
}

case class GeneratedDataset[T](partitions: List[Int], generator: (Int, Int) => List[T]) extends Dataset[T]
case class MappedDataset[T, U](upstream: Dataset[T], mapper: T => U)                    extends Dataset[U]
case class FilteredDataset[T](upstream: Dataset[T], pred: T => Boolean)                 extends Dataset[T]
case class CoalescedDataset[T](upstream: Dataset[T], partitions: List[Int])             extends Dataset[T]
case class LocalReduceDataset[T](upstream: Dataset[T], reducer: (T, T) => T)            extends Dataset[T]
