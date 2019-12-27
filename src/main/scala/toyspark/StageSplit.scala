package toyspark

import scala.annotation.tailrec

import TypeAliases._

object StageSplit {
  type StageAlt = (List[Dataset[_]], Option[PartitionSchema])

  def getActionUpstream(action: Action[_]): Dataset[_] = action match {
    case CollectAction(upstream, _)                    => upstream
    case CountAction(upstream)                         => LocalCountDataset(upstream)
    case TakeAction(upstream, _, _)                    => upstream
    case SaveAsSequenceFileAction(upstream, dir, name) => IsSavingSeqFileOkDataset(upstream, dir, name)
    case ReduceAction(upstream, reducer, _)            => LocalReduceDataset(upstream, reducer)
  }

  @tailrec
  def getPartitions(dataset: Dataset[_]): List[Int] = {
    dataset match {
      case GeneratedDataset(partitions, _)          => partitions
      case ReadDataset(partitions, _, _)            => partitions
      case MappedDataset(upstream, _)               => getPartitions(upstream)
      case FlatMappedDataset(upstream, _)           => getPartitions(upstream)
      case LocalDistinctDataset(upstream)           => getPartitions(upstream)
      case LocalGroupedByKeyDataset(upstream)       => getPartitions(upstream)
      case LocalReducedByKeyDataset(upstream, _)    => getPartitions(upstream)
      case FilteredDataset(upstream, _)             => getPartitions(upstream)
      case RepartitionDataset(_, partitions)        => partitions
      case HashShuffleDataset(upstream)             => getPartitions(upstream)
      case PairHashShuffleDataset(upstream)         => getPartitions(upstream)
      case UnionDataset(lhs, _)                     => getPartitions(lhs)
      case IntersectionDataset(lhs, _)              => getPartitions(lhs)
      case CartesianDataset(lhs, _)                 => getPartitions(lhs)
      case LocalJointDataset(lhs, _)                => getPartitions(lhs)
      case LocalCountDataset(upstream)              => getPartitions(upstream)
      case IsSavingSeqFileOkDataset(upstream, _, _) => getPartitions(upstream)
      case LocalReduceDataset(upstream, _)          => getPartitions(upstream)
    }
  }

  def splitStagesDAG[_](action: Action[_], substituteCached: Boolean): (List[Stage], Dataset[_]) = {
    val actionUpstream = getActionUpstream(action)

    def splitAux(curDs: Dataset[_], allAcc: List[Stage], curAcc: List[Dataset[_]]): List[Stage] = {
      if (substituteCached && Context.hasMemCache(Context.cread(curDs))) {
        (MemCacheDataset(curDs) :: curAcc, getPartitions(curDs)) :: allAcc
      } else {
        curDs match {
          case GeneratedDataset(partitions, _)     => (curDs :: curAcc, partitions) :: allAcc
          case ReadDataset(partitions, _, _)       => (curDs :: curAcc, partitions) :: allAcc
          case MappedDataset(ups, _)               => splitAux(ups, allAcc, curDs :: curAcc)
          case FlatMappedDataset(ups, _)           => splitAux(ups, allAcc, curDs :: curAcc)
          case LocalDistinctDataset(ups)           => splitAux(ups, allAcc, curDs :: curAcc)
          case LocalGroupedByKeyDataset(ups)       => splitAux(ups, allAcc, curDs :: curAcc)
          case LocalReducedByKeyDataset(ups, _)    => splitAux(ups, allAcc, curDs :: curAcc)
          case FilteredDataset(ups, _)             => splitAux(ups, allAcc, curDs :: curAcc)
          case RepartitionDataset(ups, partitions) => splitAux(ups, (curDs :: curAcc, partitions) :: allAcc, Nil)
          case HashShuffleDataset(u)               => splitAux(u, (curDs :: curAcc, getPartitions(u)) :: allAcc, Nil)
          case PairHashShuffleDataset(u)           => splitAux(u, (curDs :: curAcc, getPartitions(u)) :: allAcc, Nil)
          case UnionDataset(lhs, rhs)              => splitAux(rhs, Nil, Nil) ++ splitAux(lhs, allAcc, curDs :: curAcc)
          case IntersectionDataset(lhs, rhs)       => splitAux(rhs, Nil, Nil) ++ splitAux(lhs, allAcc, curDs :: curAcc)
          case CartesianDataset(lhs, rhs)          => splitAux(rhs, Nil, Nil) ++ splitAux(lhs, allAcc, curDs :: curAcc)
          case LocalJointDataset(lhs, rhs)         => splitAux(rhs, Nil, Nil) ++ splitAux(lhs, allAcc, curDs :: curAcc)
          case LocalCountDataset(ups)              => splitAux(ups, allAcc, curDs :: curAcc)
          case IsSavingSeqFileOkDataset(ups, _, _) => splitAux(ups, allAcc, curDs :: curAcc)
          case LocalReduceDataset(ups, _)          => splitAux(ups, allAcc, curDs :: curAcc)
        }
      }
    }

    (splitAux(actionUpstream, Nil, Nil), actionUpstream)
  }

  @deprecated("only for linear dependency")
  def splitStages[_](action: Action[_]): List[Stage] = {
    val actionUpstream = getActionUpstream(action)

    @tailrec
    def splitStagesAux(dataset: Dataset[_], acc: List[Stage]): List[Stage] = {
      extendToStage(dataset) match {
        case (stage, null)     => stage :: acc
        case (stage, prevLast) => splitStagesAux(prevLast, stage :: acc)
      }
    }

    splitStagesAux(actionUpstream, Nil)
  }

  @deprecated("only for linear dependency")
  def extendToStage(dataset: Dataset[_]): (Stage, Dataset[_]) = {
    @tailrec
    def extendToStageAux(dataset: Dataset[_], acc: StageAlt): (StageAlt, Dataset[_]) = (dataset, acc) match {
      case (GeneratedDataset(partitions, _), (remain, _))    => ((dataset :: remain, Some(partitions)), null)
      case (ReadDataset(partitions, _, _), (remain, _))      => ((dataset :: remain, Some(partitions)), null)
      case (MappedDataset(us, _), (remain, _))               => extendToStageAux(us, (dataset :: remain, None))
      case (FilteredDataset(us, _), (remain, _))             => extendToStageAux(us, (dataset :: remain, None))
      case (RepartitionDataset(us, partitions), (remain, _)) => ((dataset :: remain, Some(partitions)), us)
      case (LocalCountDataset(us), (remain, _))              => extendToStageAux(us, (dataset :: remain, None))
      case (LocalReduceDataset(us, _), (remain, _))          => extendToStageAux(us, (dataset :: remain, None))
      case (IsSavingSeqFileOkDataset(us, _, _), (remain, _)) => extendToStageAux(us, (dataset :: remain, None))
    }

    extendToStageAux(dataset, (Nil, None)) match {
      case ((datasets, Some(partition)), upstream) => ((datasets, partition), upstream)
      case _                                       => throw new RuntimeException("unexpected result")
    }
  }
}
