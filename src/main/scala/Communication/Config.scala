package Communication

import org.json4s.native.JsonMethods.parse

case class Config(val master: (String, Int), val workers: List[String])

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
    Config((masterIp, masterPort), workerIps)
  }
}
