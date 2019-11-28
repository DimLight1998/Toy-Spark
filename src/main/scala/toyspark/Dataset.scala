package toyspark

abstract class Dataset[T] {
  def map[U](mapper: T => U): MappedDataset[T, U]            = MappedDataset(this, mapper)
  def filter(pred: T => Boolean): FilteredDataset[T]         = FilteredDataset(this, pred)
  def coalesced(partitions: List[Int]): CoalescedDataset[T]  = CoalescedDataset(this, partitions)
  def reduce(reducer: (T, T) => T, workerRet: T): T          = ReduceAction(this, reducer, workerRet).perform()
  def collect(workerRet: List[T]): List[T]                   = CollectAction(this, workerRet).perform()
  def count(): Int                                           = CountAction(this).perform()
  def take(takeCount: Int, workerRet: List[T]): List[T]      = TakeAction(this, takeCount, workerRet).perform()
  def saveAsSequenceFile(dir: String, name: String): Boolean = SaveAsSequenceFileAction(this, dir, name).perform()
}

object Dataset {
  def generate[T](partitions: List[Int], generator: (Int, Int) => List[T]): GeneratedDataset[T] =
    GeneratedDataset(partitions, generator)
  def read[T](partitions: List[Int], dataFile: String, dtype: T): ReadDataset[T] =
    ReadDataset(partitions, dataFile, dtype)
}

case class GeneratedDataset[T](partitions: List[Int], generator: (Int, Int) => List[T])         extends Dataset[T]
case class ReadDataset[T](partitions: List[Int], dataFile: String, dtype: T)                    extends Dataset[T]
case class MappedDataset[T, U](upstream: Dataset[T], mapper: T => U)                            extends Dataset[U]
case class FilteredDataset[T](upstream: Dataset[T], pred: T => Boolean)                         extends Dataset[T]
case class CoalescedDataset[T](upstream: Dataset[T], partitions: List[Int])                     extends Dataset[T]
case class LocalCountDataset[T](upstream: Dataset[T])                                           extends Dataset[T]
case class LocalSaveAsSequenceFileDataset[T](upstream: Dataset[T], dir: String, name:String)    extends Dataset[T]
case class LocalReduceDataset[T](upstream: Dataset[T], reducer: (T, T) => T)                    extends Dataset[T]
