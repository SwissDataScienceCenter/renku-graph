organization := "ch.datascience"
scalaVersion := "2.12.7"
name := "gitlab-events-miner"

libraryDependencies += "org.postgresql" % "postgresql" % "9.3-1102-jdbc41"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"