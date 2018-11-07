enablePlugins(JavaAppPackaging)

organization := "ch.datascience"
name := "webhook-service"
version := "0.1.0"

scalaVersion := "2.12.7"

fork in runMain := true

libraryDependencies ++= {
  val akkaHttpVersion = "10.1.5"
  val akkaVersion = "2.5.12"

  Seq(
    "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-slf4j"  % akkaVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,

    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion % Test,
    "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpVersion % Test,
    "org.scalamock"     %% "scalamock"            % "4.1.0"         % Test,
    "org.scalatest"     %% "scalatest"            % "3.0.5"         % Test
  )
}
