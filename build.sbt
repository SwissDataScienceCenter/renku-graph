organization := "ch.datascience"
name := "renku-graph"
scalaVersion := "2.13.5"

// This project contains nothing to package, like pure POM maven project
packagedArtifacts := Map.empty

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseIgnoreUntrackedFiles := true
releaseTagName := (ThisBuild / version).value

lazy val root = Project(
  id = "renku-graph",
  base = file(".")
).settings(
  publish / skip := true,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
).aggregate(
  jsonLd,
  graphCommons,
  eventLog,
  tokenRepository,
  webhookService,
  commitEventService,
  triplesGenerator,
  knowledgeGraph
)

lazy val jsonLd = Project(
  id = "json-ld",
  base = file("json-ld")
).settings(
  commonSettings
).enablePlugins(
  AutomateHeaderPlugin
)

lazy val graphCommons = Project(
  id = "graph-commons",
  base = file("graph-commons")
).settings(
  commonSettings
).dependsOn(
  jsonLd % "compile->compile",
  jsonLd % "test->test"
).enablePlugins(
  AutomateHeaderPlugin
)

lazy val eventLog = Project(
  id = "event-log",
  base = file("event-log")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  JavaAppPackaging,
  AutomateHeaderPlugin
)

lazy val webhookService = Project(
  id = "webhook-service",
  base = file("webhook-service")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  JavaAppPackaging,
  AutomateHeaderPlugin
)

lazy val commitEventService = Project(
  id = "commit-event-service",
  base = file("commit-event-service")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  JavaAppPackaging,
  AutomateHeaderPlugin
)

lazy val triplesGenerator = Project(
  id = "triples-generator",
  base = file("triples-generator")
).settings(
  commonSettings
).dependsOn(
  jsonLd       % "compile->compile",
  jsonLd       % "test->test",
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  JavaAppPackaging,
  AutomateHeaderPlugin
)

lazy val tokenRepository = Project(
  id = "token-repository",
  base = file("token-repository")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  JavaAppPackaging,
  AutomateHeaderPlugin
)

lazy val knowledgeGraph = Project(
  id = "knowledge-graph",
  base = file("knowledge-graph")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  JavaAppPackaging,
  AutomateHeaderPlugin
)

lazy val acceptanceTests = Project(
  id = "acceptance-tests",
  base = file("acceptance-tests")
).settings(
  commonSettings
).dependsOn(
  webhookService,
  commitEventService,
  triplesGenerator,
  tokenRepository,
  knowledgeGraph % "test->test",
  graphCommons   % "test->test",
  eventLog       % "test->test"
).enablePlugins(
  AutomateHeaderPlugin
)

lazy val commonSettings = Seq(
  organization := "ch.datascience",
  scalaVersion := "2.13.5",
  publish / skip := true,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
  Compile / packageDoc / publishArtifact := false,
  Compile / packageSrc / publishArtifact := false,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.3" cross CrossVersion.full),
  // format: off
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding", "utf-8", // Specify character encoding used by source files.
    "-explaintypes", // Explain type errors in more detail.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-language:higherKinds", // Allow higher-kinded types
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings",
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:package-object-classes", // Class or object defined in package object.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Wdead-code", // Warn when dead code is identified.
    "-Wunused:imports", // Warn if an import selector is not referenced.
    "-Wunused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Wunused:locals", // Warn if a local definition is unused.
    "-Wunused:explicits", // Warn if an explicit parameter is unused.
    "-Wvalue-discard", // Warn when non-Unit expression results are unused.
    "-Ybackend-parallelism", "8",
    "-Ycache-plugin-class-loader:last-modified", // Enables caching of classloaders for compiler plugins
    "-Ycache-macro-class-loader:last-modified", // and macro definitions. This can lead to performance improvements.
    "-Ywarn-value-discard" // Emit warning and location for usages of deprecated APIs.
  ),
  // Format: on
  organizationName := "Swiss Data Science Center (SDSC)",
  startYear := Some(java.time.LocalDate.now().getYear),
  licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  headerLicense := Some(
    HeaderLicense.Custom(
      s"""|Copyright ${java.time.LocalDate.now().getYear} Swiss Data Science Center (SDSC)
          |A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
          |Eidgenössische Technische Hochschule Zürich (ETHZ).
          |
          |Licensed under the Apache License, Version 2.0 (the "License");
          |you may not use this file except in compliance with the License.
          |You may obtain a copy of the License at
          |
          |    http://www.apache.org/licenses/LICENSE-2.0
          |
          |Unless required by applicable law or agreed to in writing, software
          |distributed under the License is distributed on an "AS IS" BASIS,
          |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
          |See the License for the specific language governing permissions and
          |limitations under the License.""".stripMargin
    )
  )
)

import sbtrelease.Vcs

lazy val writeVersionToChart = taskKey[Unit]("Write release version to Chart.yaml")

writeVersionToChart := {
  val version = readTag

  val chartFile = root.base / "helm-chart" / "renku-graph" / "Chart.yaml"

  val fileLines = IO.readLines(chartFile)
  val updatedLines = fileLines.map {
    case line if line.startsWith("version:") => s"version: $version"
    case line                                => line
  }
  IO.writeLines(chartFile, updatedLines)
}

def readTag: String = {
  val tag = Vcs
    .detect(root.base)
    .map(_.cmd("describe", "--tags").!!.trim)
    .getOrElse(sys.error("Release Tag cannot be checked"))

  if (tag.matches("\\d+\\.\\d+\\.\\d+")) tag
  else sys.error("Current commit is not tagged")
}
