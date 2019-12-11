package toyspark

import scala.annotation.tailrec

import TypeAliases._

object StageSplit {
  type StageAlt = (List[Dataset[_]], Option[PartitionSchema])

  def splitStages[_](action: Action[_]): List[Stage] = {
    val actionUpstream = action match {
      case CollectAction(upstream, _)                    => upstream
      case CountAction(upstream)                         => LocalCountDataset(upstream)
      case TakeAction(upstream, _, _)                    => upstream
      case SaveAsSequenceFileAction(upstream, dir, name) => LocalSaveAsSequenceFileDataset(upstream, dir, name)
      case ReduceAction(upstream, reducer, _)            => LocalReduceDataset(upstream, reducer)
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
      case (GeneratedDataset(partitions, _), (remain, _))          => ((dataset :: remain, Some(partitions)), null)
      case (ReadDataset(partitions, _, _), (remain, _))            => ((dataset :: remain, Some(partitions)), null)
      case (MappedDataset(us, _), (remain, _))                     => extendToStageAux(us, (dataset :: remain, None))
      case (FilteredDataset(us, _), (remain, _))                   => extendToStageAux(us, (dataset :: remain, None))
      case (RepartitionDataset(us, partitions), (remain, _))       => ((dataset :: remain, Some(partitions)), us)
      case (LocalCountDataset(us), (remain, _))                    => extendToStageAux(us, (dataset :: remain, None))
      case (LocalReduceDataset(us, _), (remain, _))                => extendToStageAux(us, (dataset :: remain, None))
      case (LocalSaveAsSequenceFileDataset(us, _, _), (remain, _)) => extendToStageAux(us, (dataset :: remain, None))
    }

    extendToStageAux(dataset, (Nil, None)) match {
      case ((datasets, Some(partition)), upstream) => ((datasets, partition), upstream)
      case _                                       => throw new RuntimeException("unexpected result")
    }
  }
}
