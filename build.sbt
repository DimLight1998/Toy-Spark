name := "Toy-Spark"

version := "0.1"

scalaVersion := "2.12.10"

// todo #2 [L] use more serious protocol for message passing

libraryDependencies += "org.json4s"         % "json4s-native_2.12" % "3.7.0-M1"

// todo #6 [L] use more serious protocol for data serialization
libraryDependencies += "org.apache.commons" % "commons-lang3"      % "3.0"
