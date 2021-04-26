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

import sys.process._
val platform = System.getProperty("os.name").toLowerCase
val numberOfCores: String =
  if (platform.startsWith("mac os x")) "sysctl hw.logicalcpu".!!.split(" ").last
  else "nproc".!!

lazy val hello = taskKey[Unit]("Prints 'Hello World'")

hello := println(s"numberOfCores  = $numberOfCores")

lazy val commonSettings = Seq(
  organization := "ch.datascience",
  scalaVersion := "2.13.5",
  publish / skip := true,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
  Compile / packageDoc / publishArtifact := false,
  Compile / packageSrc / publishArtifact := false,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.3" cross CrossVersion.full),
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-Ywarn-value-discard", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-explaintypes", // Explain type errors in more detail.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    //  "-language:experimental.macros",   // Allow macro definition (besides implementation and application). Disabled, as this will significantly change in Scala 3
    "-language:higherKinds", // Allow higher-kinded types
    //  "-language:implicitConversions",   // Allow definition of implicit functions called views. Disabled, as it might be dropped in Scala 3. Instead use extension methods (implemented as implicit class Wrapper(val inner: Foo) extends AnyVal {}
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:implicit-recursion", // Warn when an implicit resolves to an enclosing self-definition.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:package-object-classes", // Class or object defined in package object.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Xlint:nonlocal-return", // A return statement used an exception for flow control.
    "-Xlint:implicit-not-found", // Check @implicitNotFound and @implicitAmbiguous messages.
    "-Xlint:serial", // @SerialVersionUID on traits and non-serializable classes.
    "-Xlint:valpattern", // Enable pattern checks in val definitions.
    "-Xlint:eta-zero", // Warn on eta-expansion (rather than auto-application) of zero-ary method.
    "-Xlint:eta-sam", // Warn on eta-expansion to meet a Java-defined functional interface that is not explicitly annotated with @FunctionalInterface.
    "-Xlint:deprecation", // Enable linted deprecations.
    "-Wdead-code", // Warn when dead code is identified.
    "-Wextra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Wmacros:both", // Lints code before and after applying a macro
    "-Wnumeric-widen", // Warn when numerics are widened.
    "-Woctal-literal", // Warn on obsolete octal syntax.
    "-Wunused:imports", // Warn if an import selector is not referenced.
    "-Wunused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Wunused:locals", // Warn if a local definition is unused.
    "-Wunused:explicits", // Warn if an explicit parameter is unused.
    "-Vimplicits",
    "-Wvalue-discard", // Warn when non-Unit expression results are unused.
    "-Ybackend-parallelism",
    "8",
    "-Ycache-plugin-class-loader:last-modified", // Enables caching of classloaders for compiler plugins
    "-Ycache-macro-class-loader:last-modified" // and macro definitions. This can lead to performance improvements.
  ),
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
