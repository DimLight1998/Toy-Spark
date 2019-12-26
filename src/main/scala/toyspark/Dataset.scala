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

  // actions
  def reduce(reducer: (T, T) => T, workerRet: T): T          = ReduceAction(this, reducer, workerRet).perform()
  def collect(workerRet: List[T]): List[T]                   = CollectAction(this, workerRet).perform()
  def count(): Int                                           = CountAction(this).perform()
  def take(takeCount: Int, workerRet: List[T]): List[T]      = TakeAction(this, takeCount, workerRet).perform()
  def saveAsSequenceFile(dir: String, name: String): Boolean = SaveAsSequenceFileAction(this, dir, name).perform()

  // misc.
  def save(): Unit = Context.addMemCacheMark(Context.cread(this))
}

abstract class PairDataset[T, U] extends Dataset[(T, U)] {
  implicit def convertFromDataset(dataset: Dataset[(T, U)]): PairDataset[T, U] = {
    dataset.asInstanceOf[PairDataset[T, U]]
  }

  def groupByKey(): GrouppedByKeyDataset[T, U]                     = GrouppedByKeyDataset(this)
  def join[K](other: PairDataset[T, K]): JointDataset[T, U, K]     = JointDataset(this, other)
  def reduceByKey(reducer: (U, U) => U): ReducedByKeyDataset[T, U] = ReducedByKeyDataset(this, reducer)
}

object Dataset {
  def generate[T](partitions: List[Int], generator: (Int, Int) => List[T]): GeneratedDataset[T] =
    GeneratedDataset(partitions, generator)
  def read[T](partitions: List[Int], dataFile: String, dtype: T): ReadDataset[T] =
    ReadDataset(partitions, dataFile, dtype)
}

case class GeneratedDataset[T](partitions: List[Int], generator: (Int, Int) => List[T]) extends Dataset[T]
case class ReadDataset[T](partitions: List[Int], dataFile: String, dtype: T)            extends Dataset[T]
case class MappedDataset[T, U](upstream: Dataset[T], mapper: T => U)                    extends Dataset[U]
case class FlatMappedDataset[T, U](upstream: Dataset[T], mapper: T => Iterable[U])      extends Dataset[U]
case class FilteredDataset[T](upstream: Dataset[T], pred: T => Boolean)                 extends Dataset[T]
case class RepartitionDataset[T](upstream: Dataset[T], partitions: List[Int])           extends Dataset[T]
case class HashShuffleDataset[T](upstream: Dataset[T])                                  extends Dataset[T]
case class LocalDistinctDataset[T](upstream: HashShuffleDataset[T])                     extends Dataset[T]
case class UnionDataset[T](lhs: Dataset[T], rhs: Dataset[T])                            extends Dataset[T]
case class IntersectionDataset[T](lhs: Dataset[T], rhs: Dataset[T])                     extends Dataset[T]
case class CartesianDataset[T, U](lhs: Dataset[T], rhs: Dataset[U])                     extends Dataset[(T, U)]
case class LocalCountDataset[T](upstream: Dataset[T])                                   extends Dataset[T]
case class IsSavingSeqFileOkDataset[T](upstream: Dataset[T], dir: String, name: String) extends Dataset[T]
case class LocalReduceDataset[T](upstream: Dataset[T], reducer: (T, T) => T)            extends Dataset[T]
case class MemCacheDataset[T](wrapping: Dataset[T])                                     extends Dataset[T]
case class GrouppedByKeyDataset[T, U](upstream: PairDataset[T, U])                      extends PairDataset[T, U]
case class JointDataset[T, U, K](lhs: PairDataset[T, U], rhs: PairDataset[T, K])        extends PairDataset[T, (U, K)]
case class ReducedByKeyDataset[T, U](upstream: PairDataset[T, U], reducer: (U, U) => U) extends PairDataset[T, U]
