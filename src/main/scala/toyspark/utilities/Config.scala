package toyspark.utilities

import org.json4s.native.JsonMethods.parse

final case class Config(master: (String, Int), workers: List[String]) {
  def getNodesIPs: List[String] = master._1 :: workers
}

object Config {
  def fromJsonFile(pathname: String): Config = {
    val source = scala.io.Source.fromFile(pathname)
    val lines  = source.getLines().mkString("")
    val config = parse(lines)
    source.close()

    val masterIp   = (config \ "master" \ "ip").values.asInstanceOf[String]
    val masterPort = Integer.parseInt((config \ "master" \ "port").values.asInstanceOf[String])
    val workers    = (config \ "workers").values.asInstanceOf[List[Map[String, String]]]
    val workerIps  = workers.map(x => x("ip"))
    HDFSUtil.coreSitePath = (config \ "hdfs" \ "coreSitePath").values.asInstanceOf[String]
    HDFSUtil.hdfsUrl = (config \ "hdfs" \ "url").values.asInstanceOf[String]
    Config((masterIp, masterPort), workerIps)
  }
}
