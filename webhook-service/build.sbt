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
    "com.lihaoyi"       %% "ammonite-ops"         % "1.4.2",
    "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-slf4j"           % akkaVersion,
    "com.typesafe.akka" %% "akka-stream"          % akkaVersion,
    "org.typelevel"     %% "cats-core"            % "1.4.0",

    "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpVersion % Test,
    "org.scalacheck"    %% "scalacheck"           % "1.14.0"        % Test,
    "org.scalamock"     %% "scalamock"            % "4.1.0"         % Test,
    "org.scalatest"     %% "scalatest"            % "3.0.5"         % Test
  )
}
