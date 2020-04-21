// format: off
organization := "ch.datascience"
name := "renku-graph"
scalaVersion := "2.12.8"

// This project contains nothing to package, like pure POM maven project
packagedArtifacts := Map.empty

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseIgnoreUntrackedFiles := true
releaseTagName := (version in ThisBuild).value.toString

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

lazy val triplesGenerator = Project(
  id = "triples-generator",
  base = file("triples-generator")
).settings(
  commonSettings
).dependsOn(
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
  triplesGenerator,
  tokenRepository,
  knowledgeGraph % "test->test",
  graphCommons % "test->test",
  eventLog % "test->test"
).enablePlugins(
  AutomateHeaderPlugin
)

lazy val commonSettings = Seq(
  organization := "ch.datascience",
  scalaVersion := "2.12.8",

  skip in publish := true,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),

  publishArtifact in(Compile, packageDoc) := false,
  publishArtifact in(Compile, packageSrc) := false,

  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3"),

  scalacOptions += "-Ypartial-unification",
  scalacOptions += "-feature",
  scalacOptions += "-unchecked",
  scalacOptions += "-deprecation",
  scalacOptions += "-Ywarn-value-discard",
  scalacOptions += "-Xfatal-warnings",

  organizationName := "Swiss Data Science Center (SDSC)",
  startYear := Some(java.time.LocalDate.now().getYear),
  licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  headerLicense := Some(HeaderLicense.Custom(
    s"""Copyright ${java.time.LocalDate.now().getYear} Swiss Data Science Center (SDSC)
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
  ))
)
// format: on

import ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.{Vcs, Versions}

releaseTagComment := {
  Vcs.detect(root.base).map { implicit vcs =>
    collectCommitsMessages() match {
      case Nil => s"Releasing ${(version in ThisBuild).value}"
      case messages =>
        s"Release Notes for ${(version in ThisBuild).value}\n${messages.map(m => s"* $m").mkString("\n")}"
    }
  }
}.getOrElse {
  sys.error("Release Tag comment cannot be calculated")
}

@scala.annotation.tailrec
def collectCommitsMessages(commitsCounter: Int          = 1,
                           messages:       List[String] = List.empty)(implicit vcs: Vcs): List[String] =
  vcs.cmd("log", "--format=%s", "-1", s"HEAD~$commitsCounter").!!.trim match {
    case message if message startsWith "Setting version" => messages
    case message                                         => collectCommitsMessages(commitsCounter + 1, messages :+ message)
  }

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  setReleaseVersionToChart,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  setNextVersionToChart,
  commitNextVersion,
  pushChanges
)

val setReleaseVersionToChart: ReleaseStep = setReleaseVersionChart(_._1)
val setNextVersionToChart:    ReleaseStep = setNextReleaseVersionChart(_._2)

def setReleaseVersionChart(selectVersion: Versions => String): ReleaseStep = { state: State =>
  val version = findVersion(selectVersion, state)

  updateAndCommitChart(version)

  state
}

def setNextReleaseVersionChart(selectVersion: Versions => String): ReleaseStep = { state: State =>
  val nextVersion = findVersion(selectVersion, state)
  val currentHash = Vcs.detect(root.base).map(_.currentHash).getOrElse(sys.error("Current hash cannot be obtained"))
  val version     = nextVersion.replace("SNAPSHOT", currentHash.take(7))

  updateAndCommitChart(version)

  state
}

def findVersion(selectVersion: Versions => String, state: State) = {
  val allVersions = state.get(versions).getOrElse {
    sys.error("No versions are set! Was this release part executed before inquireVersions?")
  }

  selectVersion(allVersions)
}

def updateAndCommitChart(version: String): Unit = {
  writeChartVersion(version)

  addChartToVcs()
}

val chartFile = root.base / "helm-chart" / "renku-graph" / "Chart.yaml"

def addChartToVcs(): Unit =
  for {
    vcs      <- Vcs.detect(root.base)
    filePath <- IO.relativize(root.base, chartFile)
  } yield vcs.add(filePath).run()

def writeChartVersion(version: String): Unit = {

  val fileLines = IO.readLines(chartFile)
  val updatedLines = fileLines.map {
    case line if line.startsWith("version:") => s"version: $version"
    case line                                => line
  }
  IO.writeLines(chartFile, updatedLines)
}
