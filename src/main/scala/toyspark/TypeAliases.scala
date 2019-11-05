package toyspark

case object TypeAliases {
  type PartitionSchema = List[Int]
  type Stage           = (List[Dataset[_]], PartitionSchema)
}
