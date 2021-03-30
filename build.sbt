organization := "ch.datascience"
name := "renku-graph"
scalaVersion := "2.13.4"

// This project contains nothing to package, like pure POM maven project
packagedArtifacts := Map.empty

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseIgnoreUntrackedFiles := true
releaseTagName := (version in ThisBuild).value

lazy val root = Project(
  id = "renku-graph",
  base = file(".")
).settings(
  skip in publish := true,
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
  scalaVersion := "2.13.4",
  skip in publish := true,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.1" cross CrossVersion.full),
  scalacOptions += "-feature",
  scalacOptions += "-unchecked",
  scalacOptions += "-deprecation",
  scalacOptions += "-Ywarn-value-discard",
  scalacOptions += "-Xfatal-warnings",
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
    .map(_.cmd("describe").!!.trim)
    .getOrElse(sys.error("Release Tag cannot be checked"))

  if (tag.matches("\\d+\\.\\d+\\.\\d+")) tag
  else sys.error("Current commit is not tagged")
}
