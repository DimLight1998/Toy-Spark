import scala.annotation.tailrec

package object Datasets {
  type PartitionSchema = List[Int]
  type Stage           = (List[Dataset[_]], PartitionSchema)
  type StageAlt        = (List[Dataset[_]], Option[PartitionSchema])

  def splitStages[_](action: Action[_]): List[Stage] = {
    val actionUpstream = action match {
      case CollectAction(upstream)   => upstream
      case ReduceAction(upstream, _) => upstream
    }

    @tailrec
    def splitStagesAux(dataset: Dataset[_], acc: List[Stage]): List[Stage] = {
      extendToStage(dataset) match {
        case (stage, null)     => stage :: acc
        case (stage, prevLast) => splitStagesAux(prevLast, stage :: acc)
      }
    }

    splitStagesAux(actionUpstream, Nil)
  }

  def extendToStage(dataset: Dataset[_]): (Stage, Dataset[_]) = {
    @tailrec
    def extendToStageAux(dataset: Dataset[_], acc: StageAlt): (StageAlt, Dataset[_]) = (dataset, acc) match {
      case (GeneratedDataset(partitions, _), (remain, _))  => ((dataset :: remain, Some(partitions)), null)
      case (MappedDataset(us, _), (remain, _))             => extendToStageAux(us, (dataset :: remain, None))
      case (FilteredDataset(us, _), (remain, _))           => extendToStageAux(us, (dataset :: remain, None))
      case (CoalescedDataset(us, partitions), (remain, _)) => ((dataset :: remain, Some(partitions)), us)
    }

    extendToStageAux(dataset, (Nil, None)) match {
      case ((datasets, Some(partition)), upstream) => ((datasets, partition), upstream)
      case _                                       => throw new RuntimeException("unexpected result")
    }
  }
}
