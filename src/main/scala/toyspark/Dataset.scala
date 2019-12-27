package toyspark

abstract class Dataset[T] {
  // transformations
  def map[U](mapper: T => U): MappedDataset[T, U]                   = MappedDataset(this, mapper)
  def flatMap[U](mapper: T => Iterable[U]): FlatMappedDataset[T, U] = FlatMappedDataset(this, mapper)
  def filter(pred: T => Boolean): FilteredDataset[T]                = FilteredDataset(this, pred)
  def repartition(partitions: List[Int]): RepartitionDataset[T]     = RepartitionDataset(this, partitions)
  def distinct(): LocalDistinctDataset[T]                           = LocalDistinctDataset(HashShuffleDataset(this))
  def unionWith(other: Dataset[T]): UnionDataset[T]                 = UnionDataset(this, other)
  def intersectionWith(other: Dataset[T]): IntersectionDataset[T]   = IntersectionDataset(this, other)
  def cartesianWith[U](other: Dataset[U]): CartesianDataset[T, U]   = CartesianDataset(this, other)
  def joinWith(other: Dataset[_]): LocalJointDataset[_] =
    LocalJointDataset(PairHashShuffleDataset(this), other)
  def groupByKey(): LocalGroupedByKeyDataset[Any] =
    LocalGroupedByKeyDataset(PairHashShuffleDataset(this))
  def reduceByKey(reducer: (Any, Any) => Any): LocalReducedByKeyDataset[Any] =
    LocalReducedByKeyDataset(PairHashShuffleDataset(this), reducer)

  // actions
  def reduce(reducer: (T, T) => T, workerRet: T): T          = ReduceAction(this, reducer, workerRet).perform()
  def collect(workerRet: List[T]): List[T]                   = CollectAction(this, workerRet).perform()
  def count(): Int                                           = CountAction(this).perform()
  def take(takeCount: Int, workerRet: List[T]): List[T]      = TakeAction(this, takeCount, workerRet).perform()
  def saveAsSequenceFile(dir: String, name: String): Boolean = SaveAsSequenceFileAction(this, dir, name).perform()

  // misc.
  def save(): Unit = Context.addMemCacheMark(Context.cread(this))
}

object Dataset {
  def generate[T](partitions: List[Int], generator: (Int, Int) => List[T]): GeneratedDataset[T] =
    GeneratedDataset(partitions, generator)
  def read[T](partitions: List[Int], dataFile: String, dtype: T): ReadDataset[T] =
    ReadDataset(partitions, dataFile, dtype)
}

case class GeneratedDataset[T](partitions: List[Int], generator: (Int, Int) => List[T])  extends Dataset[T]
case class ReadDataset[T](partitions: List[Int], dataFile: String, dtype: T)             extends Dataset[T]
case class MappedDataset[T, U](upstream: Dataset[T], mapper: T => U)                     extends Dataset[U]
case class FlatMappedDataset[T, U](upstream: Dataset[T], mapper: T => Iterable[U])       extends Dataset[U]
case class FilteredDataset[T](upstream: Dataset[T], pred: T => Boolean)                  extends Dataset[T]
case class RepartitionDataset[T](upstream: Dataset[T], partitions: List[Int])            extends Dataset[T]
case class HashShuffleDataset[T](upstream: Dataset[T])                                   extends Dataset[T]
case class LocalDistinctDataset[T](upstream: Dataset[T])                                 extends Dataset[T]
case class UnionDataset[T](lhs: Dataset[T], rhs: Dataset[T])                             extends Dataset[T]
case class IntersectionDataset[T](lhs: Dataset[T], rhs: Dataset[T])                      extends Dataset[T]
case class CartesianDataset[T, U](lhs: Dataset[T], rhs: Dataset[U])                      extends Dataset[(T, U)]
case class LocalCountDataset[T](upstream: Dataset[T])                                    extends Dataset[T]
case class IsSavingSeqFileOkDataset[T](upstream: Dataset[T], dir: String, name: String)  extends Dataset[T]
case class LocalReduceDataset[T](upstream: Dataset[T], reducer: (T, T) => T)             extends Dataset[T]
case class MemCacheDataset[T](wrapping: Dataset[T])                                      extends Dataset[T]
case class PairHashShuffleDataset[T](upstream: Dataset[_])                               extends Dataset[T]
case class LocalGroupedByKeyDataset[T](upstream: Dataset[_])                             extends Dataset[T]
case class LocalReducedByKeyDataset[T](upstream: Dataset[_], reducer: (Any, Any) => Any) extends Dataset[T]
case class LocalJointDataset[T](lhs: Dataset[_], rhs: Dataset[_])                        extends Dataset[T]
