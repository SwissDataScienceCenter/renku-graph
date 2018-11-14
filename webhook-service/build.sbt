enablePlugins(JavaAppPackaging)

organization := "ch.datascience"
name := "webhook-service"
version := "0.1.0"

scalaVersion := "2.12.7"

fork in runMain := true

resolvers += "bblfish-snapshots" at "http://bblfish.net/work/repo/releases"

libraryDependencies ++= {
  val akkaHttpVersion = "10.1.5"
  val akkaVersion = "2.5.12"
  val bananaRdfVersion = "0.8.4"

  Seq(
    "com.lihaoyi"       %% "ammonite-ops"         % "1.4.2",
    "org.typelevel"     %% "cats-core"            % "1.4.0",
    "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-slf4j"           % akkaVersion,
    "com.typesafe.akka" %% "akka-stream"          % akkaVersion,
    "org.w3"            %% "banana"               % bananaRdfVersion excludeAll ExclusionRule(organization = "org.scala-stm"),
    "org.w3"            %% "banana-rdf"           % bananaRdfVersion excludeAll ExclusionRule(organization = "org.scala-stm"),
    "org.w3"            %% "banana-jena"          % bananaRdfVersion excludeAll ExclusionRule(organization = "org.scala-stm"),

    "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpVersion % Test,
    "org.scalacheck"    %% "scalacheck"           % "1.14.0"        % Test,
    "org.scalamock"     %% "scalamock"            % "4.1.0"         % Test,
    "org.scalatest"     %% "scalatest"            % "3.0.5"         % Test
  )
}
