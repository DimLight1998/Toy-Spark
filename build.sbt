name := "Toy-Spark"

version := "0.2"

scalaVersion := "2.12.10"

// todo #2 [L] use more serious protocol for message passing
libraryDependencies += "org.json4s"         % "json4s-native_2.12" % "3.7.0-M1"

// todo #6 [L] use more serious protocol for data serialization
libraryDependencies += "org.apache.commons" % "commons-lang3"      % "3.0"

libraryDependencies += "org.apache.hadoop"  % "hadoop-common"      % "2.7.7"

libraryDependencies += "org.apache.hadoop"  % "hadoop-hdfs"        % "2.7.7"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}