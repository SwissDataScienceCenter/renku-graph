/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

organization := "io.renku"
name := "renku-graph"
ThisBuild / scalaVersion := "2.13.12"

// This project contains nothing to package, like pure POM maven project
packagedArtifacts := Map.empty

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseIgnoreUntrackedFiles := true
releaseTagName := (ThisBuild / version).value

lazy val root = project
  .in(file("."))
  .withId("renku-graph")
  .settings(
    publish / skip := true,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
  )
  .aggregate(
    utils,
    generators,
    triplesStoreClient,
    tinyTypes,
    renkuModelTinyTypes,
    renkuCliModel,
    renkuModel,
    graphCommons,
    eventsQueue,
    tokenRepositoryApi,
    tokenRepository,
    eventLogApi,
    eventLog,
    webhookService,
    commitEventService,
    triplesGeneratorApi,
    entitiesSearch,
    entitiesViewingsCollector,
    projectAuth,
    triplesGenerator,
    renkuCoreClient,
    knowledgeGraph
  )

lazy val commonUtils = project
  .in(file("utils/common"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "common-utils",
    libraryDependencies ++=
      Dependencies.fs2Core ++
        Dependencies.catsEffect ++
        Dependencies.refined ++
        Dependencies.log4Cats ++
        Dependencies.monocle ++
        Dependencies.ip4s,
    libraryDependencies ++=
      Dependencies.scalamock.map(_ % Test)
  )
  .dependsOn(tinyTypes % "compile->compile;test->test")

lazy val sentryUtils = project
  .in(file("utils/sentry"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "sentry-utils",
    libraryDependencies ++=
      Dependencies.sentryLogback
  )
  .dependsOn(configUtils % "compile->compile;test->test")

lazy val jsonldUtils = project
  .in(file("utils/jsonld"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "jsonld-utils",
    libraryDependencies ++=
      Dependencies.jsonld4s
  )
  .dependsOn(
    tinyTypes % "compile->compile;test->test",
    commonUtils % "compile->compile;test->test"
  )

lazy val metricUtils = project
  .in(file("utils/metrics"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "metrics-utils",
    libraryDependencies ++=
      Dependencies.prometheus
  )
  .dependsOn(commonUtils % "compile->compile;test->test")

lazy val http4sClientUtils = project
  .in(file("utils/http4s-client"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "http4s-client-utils",
    libraryDependencies ++=
      Dependencies.http4sClient ++
        Dependencies.http4sCirce ++
        Dependencies.http4sDsl,
    libraryDependencies ++=
      (Dependencies.wiremock ++ Dependencies.scalamock).map(_ % Test)
  )
  .dependsOn(
    commonUtils % "compile->compile;test->test",
    metricUtils % "compile->compile;test->test"
  )

lazy val http4sServerUtils = project
  .in(file("utils/http4s-server"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "http4s-server-utils",
    libraryDependencies ++=
      Dependencies.http4sClient ++
        Dependencies.http4sServer ++
        Dependencies.http4sCirce ++
        Dependencies.http4sDsl ++
        Dependencies.http4sPrometheus
  )
  .dependsOn(
    commonUtils       % "compile->compile;test->test",
    metricUtils       % "compile->compile;test->test",
    http4sClientUtils % "compile->compile;test->test"
  )

lazy val gitlabUtils = project
  .in(file("utils/gitlab"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "gitlab-utils"
  )
  .dependsOn(
    commonUtils       % "compile->compile;test->test",
    metricUtils       % "compile->compile;test->test",
    http4sClientUtils % "compile->compile;test->test"
  )

lazy val configUtils = project
  .in(file("utils/config"))
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "config-utils",
    libraryDependencies ++=
      Dependencies.pureconfig ++
        Dependencies.refinedPureconfig ++
        Dependencies.log4Cats
  )
  .dependsOn(
    tinyTypes   % "compile->compile;test->test",
    commonUtils % "compile->compile;test->test",
    metricUtils % "compile->compile;test->test"
  )

lazy val utils = project
  .in(file("utils"))
  .aggregate(
    commonUtils,
    configUtils,
    sentryUtils,
    http4sClientUtils,
    http4sServerUtils,
    jsonldUtils,
    metricUtils,
    gitlabUtils
  )

lazy val generators = project
  .in(file("generators"))
  .withId("generators")
  .settings(commonSettings)
  .settings(
    name := "generators",
    libraryDependencies ++=
      (Dependencies.refined ++
        Dependencies.circeCore ++
        Dependencies.jsonld4s ++
        Dependencies.catsCore ++
        Dependencies.fs2Core ++
        Dependencies.scalacheck).map(_ % Test) ++
        Dependencies.ip4s
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val triplesStoreClient = project
  .in(file("triples-store-client"))
  .withId("triples-store-client")
  .settings(commonSettings)
  .settings(
    name := "triples-store-client",
    Test / testOptions += Tests.Setup(JenaServer.triplesStoreClient("start")),
    Test / testOptions += Tests.Cleanup(JenaServer.triplesStoreClient("forceStop")),
    libraryDependencies ++=
      Dependencies.jsonld4s ++
        Dependencies.luceneQueryParser ++
        Dependencies.luceneAnalyzer ++
        Dependencies.fs2Core ++
        Dependencies.rdf4jQueryParserSparql ++
        Dependencies.http4sClient ++
        Dependencies.http4sCirce,
    libraryDependencies ++=
      Dependencies.testContainersScalaTest.map(_ % Test)
  )
  .dependsOn(
    generators % "test->test",
    tinyTypes  % "compile->compile; test->test",
    http4sClientUtils % "compile->compile;test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val tinyTypes = project
  .in(file("tiny-types"))
  .withId("tiny-types")
  .settings(commonSettings)
  .settings(
    name := "tiny-types",
    libraryDependencies ++=
      Dependencies.refined ++
        Dependencies.circeCore ++
        Dependencies.circeLiteral ++
        Dependencies.circeGeneric ++
        Dependencies.circeOptics ++
        Dependencies.circeParser ++
        Dependencies.catsCore ++
        Dependencies.catsFree
  )
  .dependsOn(generators % "test->test")
//  .dependsOn(triplesStoreClient % "compile->compile; test->test") <-- why this?
  .enablePlugins(AutomateHeaderPlugin)

lazy val renkuModelTinyTypes = project
  .in(file("renku-model-tiny-types"))
  .withId("renku-model-tiny-types")
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "renku-model-tiny-types",
    libraryDependencies ++= Dependencies.jsonld4s ++ Dependencies.diffx.map(_ % Test)
  )
  .dependsOn(tinyTypes % "compile->compile; test->test", triplesStoreClient, jsonldUtils)

lazy val renkuCliModel = project
  .in(file("renku-cli-model"))
  .withId("renku-cli-model")
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    name := "renku-cli-model",
    libraryDependencies ++= Dependencies.diffx.map(_ % Test)
  )
  .dependsOn(
    tinyTypes           % "compile->compile; test->test",
    renkuModelTinyTypes % "compile->compile; test->test"
  )

lazy val renkuModel = project
  .in(file("renku-model"))
  .withId("renku-model")
  .settings(commonSettings)
  .settings(
    name := "renku-model",
    libraryDependencies ++=
      Dependencies.diffx.map(_ % Test)
  )
  .dependsOn(
    tinyTypes           % "compile->compile; test->test",
    renkuModelTinyTypes % "compile->compile; test->test",
    renkuCliModel       % "compile->compile; test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val graphCommons = project
  .in(file("graph-commons"))
  .withId("graph-commons")
  .settings(commonSettings)
  .settings(
    name := "graph-commons",
    Test / testOptions += Tests.Setup { cl =>
      PostgresServer.commons("start")
      JenaServer.commons("start")
    },
    Test / testOptions += Tests.Cleanup { cl =>
      PostgresServer.commons("forceStop")
      JenaServer.commons("forceStop")
    },
    libraryDependencies ++=
      Dependencies.pureconfig ++
        Dependencies.refinedPureconfig ++
        Dependencies.sentryLogback ++
        Dependencies.luceneQueryParser ++
        Dependencies.http4sClient ++
        Dependencies.http4sServer ++
        Dependencies.http4sCirce ++
        Dependencies.http4sDsl ++
        Dependencies.http4sPrometheus ++
        Dependencies.skunk ++
        Dependencies.catsEffect ++
        Dependencies.log4Cats,
    // Test dependencies
    libraryDependencies ++=
      (Dependencies.testContainersPostgres ++
        Dependencies.wiremock ++
        Dependencies.scalamock).map(_ % Test)
  )
  .dependsOn(
    renkuModel  % "compile->compile; test->test",
    projectAuth % "compile->compile; test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(
    commonUtils       % "compile->compile; test->test",
    configUtils       % "compile->compile; test->test",
    sentryUtils       % "compile->compile; test->test",
    http4sClientUtils % "compile->compile; test->test",
    http4sServerUtils % "compile->compile; test->test",
    jsonldUtils       % "compile->compile; test->test",
    metricUtils       % "compile->compile; test->test",
    gitlabUtils       % "compile->compile; test->test"
  )

lazy val eventsQueue = project
  .in(file("events-queue"))
  .withId("events-queue")
  .settings(commonSettings)
  .settings(
    name := "events-queue",
    Test / testOptions += Tests.Setup(PostgresServer.eventsQueue("start")),
    Test / testOptions += Tests.Cleanup(PostgresServer.eventsQueue("forceStop"))
  )
  .dependsOn(
    graphCommons % "compile->compile; test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val eventLogApi = project
  .in(file("event-log-api"))
  .withId("event-log-api")
  .settings(commonSettings)
  .settings(
    name := "event-log-api",
    libraryDependencies ++=
      Dependencies.circeGeneric ++
        Dependencies.circeGenericExtras
  )
  .dependsOn(graphCommons % "compile->compile; test->test")
  .enablePlugins(AutomateHeaderPlugin)

lazy val eventLog = project
  .in(file("event-log"))
  .withId("event-log")
  .settings(commonSettings)
  .settings(
    name := "event-log",
    Test / testOptions += Tests.Setup(PostgresServer.eventLog("start")),
    Test / testOptions += Tests.Cleanup(PostgresServer.eventLog("forceStop")),
    libraryDependencies ++= Dependencies.logbackClassic ++ Dependencies.circeGenericExtras
  )
  .dependsOn(
    tokenRepositoryApi  % "compile->compile; test->test",
    eventLogApi         % "compile->compile; test->test",
    triplesGeneratorApi % "compile->compile; test->test"
  )
  .enablePlugins(
    JavaAppPackaging,
    AutomateHeaderPlugin
  )

lazy val webhookServiceApi = project
  .in(file("webhook-service-api"))
  .withId("webhook-service-api")
  .settings(commonSettings)
  .settings(
    name := "webhook-service-api"
  )
  .dependsOn(graphCommons % "compile->compile; test->test")
  .enablePlugins(AutomateHeaderPlugin)

lazy val webhookService = project
  .in(file("webhook-service"))
  .withId("webhook-service")
  .settings(commonSettings)
  .settings(
    name := "webhook-service",
    libraryDependencies ++= Dependencies.logbackClassic
  )
  .dependsOn(
    tokenRepositoryApi  % "compile->compile; test->test",
    webhookServiceApi   % "compile->compile; test->test",
    eventLogApi         % "compile->compile; test->test",
    triplesGeneratorApi % "compile->compile; test->test"
  )
  .enablePlugins(
    JavaAppPackaging,
    AutomateHeaderPlugin
  )

lazy val commitEventService = project
  .in(file("commit-event-service"))
  .withId("commit-event-service")
  .settings(commonSettings)
  .settings(
    name := "commit-event-service",
    libraryDependencies ++= Dependencies.logbackClassic
  )
  .dependsOn(
    tokenRepositoryApi % "compile->compile; test->test",
    eventLogApi        % "compile->compile; test->test"
  )
  .enablePlugins(
    JavaAppPackaging,
    AutomateHeaderPlugin
  )

lazy val entitiesSearch = project
  .in(file("entities-search"))
  .withId("entities-search")
  .settings(commonSettings)
  .settings(
    name := "entities-search"
  )
  .dependsOn(graphCommons % "compile->compile; test->test")
  .enablePlugins(AutomateHeaderPlugin)

lazy val projectAuth = project
  .in(file("project-auth"))
  .withId("project-auth")
  .settings(commonSettings)
  .settings(
    name := "project-auth",
    Test / testOptions += Tests.Setup(JenaServer.projectAuth("start")),
    Test / testOptions += Tests.Cleanup(JenaServer.projectAuth("forceStop")),
    libraryDependencies ++= Dependencies.http4sClient
  )
  .dependsOn(
    renkuModelTinyTypes % "compile->compile; test->test",
    triplesStoreClient  % "compile->compile; test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val triplesGeneratorApi = project
  .in(file("triples-generator-api"))
  .withId("triples-generator-api")
  .settings(commonSettings)
  .settings(
    name := "triples-generator-api"
  )
  .dependsOn(graphCommons % "compile->compile; test->test")
  .enablePlugins(AutomateHeaderPlugin)

lazy val entitiesViewingsCollector = project
  .in(file("entities-viewings-collector"))
  .withId("entities-viewings-collector")
  .settings(commonSettings)
  .settings(
    name := "entities-viewings-collector",
    Test / testOptions += Tests.Setup(JenaServer.viewingsCollector("start")),
    Test / testOptions += Tests.Cleanup(JenaServer.viewingsCollector("forceStop"))
  )
  .dependsOn(
    eventsQueue         % "compile->compile; test->test",
    triplesGeneratorApi % "compile->compile; test->test",
    entitiesSearch      % "compile->compile; test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val triplesGenerator = project
  .in(file("triples-generator"))
  .withId("triples-generator")
  .settings(commonSettings)
  .settings(
    name := "triples-generator",
    Test / testOptions += Tests.Setup(JenaServer.triplesGenerator("start")),
    Test / testOptions += Tests.Cleanup(JenaServer.triplesGenerator("forceStop")),
    libraryDependencies ++=
      Dependencies.logbackClassic ++
        Dependencies.ammoniteOps,
    reStart / envVars := Map(
      "JENA_RENKU_PASSWORD" -> "renku",
      "JENA_ADMIN_PASSWORD" -> "renku",
      "RENKU_URL"           -> "http://localhost:3000"
    )
  )
  .dependsOn(
    tokenRepositoryApi  % "compile->compile; test->test",
    eventLogApi         % "compile->compile; test->test",
    triplesGeneratorApi % "compile->compile; test->test",
    entitiesSearch,
    entitiesViewingsCollector % "compile->compile; test->test",
    projectAuth               % "compile->compile; test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val tokenRepositoryApi = project
  .in(file("token-repository-api"))
  .withId("token-repository-api")
  .settings(commonSettings)
  .settings(
    name := "token-repository-api"
  )
  .dependsOn(graphCommons % "compile->compile; test->test")
  .enablePlugins(AutomateHeaderPlugin)

lazy val tokenRepository = project
  .in(file("token-repository"))
  .withId("token-repository")
  .settings(commonSettings)
  .settings(
    name := "token-repository",
    Test / testOptions += Tests.Setup(PostgresServer.tokenRepository("start")),
    Test / testOptions += Tests.Cleanup(PostgresServer.tokenRepository("forceStop")),
    libraryDependencies ++= Dependencies.logbackClassic
  )
  .dependsOn(
    tokenRepositoryApi % "compile->compile; test->test",
    eventLogApi        % "compile->compile; test->test"
  )
  .enablePlugins(
    JavaAppPackaging,
    AutomateHeaderPlugin
  )

lazy val renkuCoreClient = project
  .in(file("renku-core-client"))
  .withId("renku-core-client")
  .settings(commonSettings)
  .dependsOn(graphCommons % "compile->compile; test->test")
  .enablePlugins(AutomateHeaderPlugin)

lazy val knowledgeGraph = project
  .in(file("knowledge-graph"))
  .withId("knowledge-graph")
  .settings(commonSettings)
  .settings(
    name := "knowledge-graph",
    Test / fork := true,
    libraryDependencies ++=
      Dependencies.logbackClassic ++
        Dependencies.widoco ++
        Dependencies.swaggerParser,
    resolvers += "jitpack" at "https://jitpack.io",
    reStart / envVars := Map(
      "JENA_RENKU_PASSWORD"      -> "renku",
      "RENKU_URL"                -> "http://localhost:3000",
      "RENKU_API_URL"            -> "http://localhost:3000/api",
      "RENKU_CLIENT_CERTIFICATE" -> ""
    )
  )
  .dependsOn(
    tokenRepositoryApi  % "compile->compile; test->test",
    eventLogApi         % "compile->compile; test->test",
    entitiesSearch      % "compile->compile; test->test",
    webhookServiceApi   % "compile->compile; test->test",
    triplesGeneratorApi % "compile->compile; test->test",
    renkuCoreClient     % "compile->compile; test->test",
    entitiesViewingsCollector
  )
  .enablePlugins(
    JavaAppPackaging,
    AutomateHeaderPlugin
  )

lazy val acceptanceTests = project
  .in(file("acceptance-tests"))
  .withId("acceptance-tests")
  .settings(commonSettings)
  .settings(
    name := "acceptance-tests",
    Test / parallelExecution := false
  )
  .dependsOn(
    webhookService,
    commitEventService,
    triplesGenerator,
    tokenRepository,
    knowledgeGraph % "test->test",
    graphCommons   % "test->test",
    eventLog       % "test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val commonSettings = Seq(
  organization := "io.renku",
  publish / skip := true,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
  Compile / packageDoc / publishArtifact := false,
  Compile / packageSrc / publishArtifact := false,
  addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.2" cross CrossVersion.full),
  // format: off
  scalacOptions ++= Seq(
    "-language:postfixOps", // enabling postfixes
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
  Compile / console / scalacOptions := (Compile / scalacOptions).value.filterNot(_ == "-Xfatal-warnings"),
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
  libraryDependencies ++=  (Dependencies.scalacheck ++
    Dependencies.scalatest ++
    Dependencies.catsEffectScalaTest ++
    Dependencies.catsEffectMunit ++
    Dependencies.scalacheckEffectMunit ++
    Dependencies.scalatestScalaCheck).map(_ % Test),
  // Format: on
  organizationName := "Swiss Data Science Center (SDSC)",
  startYear := Some(java.time.LocalDate.now().getYear),
  licenses += ("Apache-2.0", new URI("https://www.apache.org/licenses/LICENSE-2.0.txt").toURL),
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

lazy val writeVersionToVersionConf = taskKey[Unit]("Write release version to version.conf files")
writeVersionToVersionConf := writeVersion(
  lineToChange = "version",
  version => s"""version = "$version"""",
  streams.value.log,
  eventLog.base / "src" / "main" / "resources" / "version.conf",
  tokenRepository.base / "src" / "main" / "resources" / "version.conf",
  webhookService.base / "src" / "main" / "resources" / "version.conf",
  commitEventService.base / "src" / "main" / "resources" / "version.conf",
  triplesGenerator.base / "src" / "main" / "resources" / "version.conf",
  knowledgeGraph.base / "src" / "main" / "resources" / "version.conf"
)

lazy val writeVersionToVersionSbt = taskKey[Unit]("Write release version to version.sbt file")
writeVersionToVersionSbt := writeVersion(
  lineToChange = "ThisBuild / version",
  version => s"""ThisBuild / version := "$version"""",
  streams.value.log,
  root.base / "version.sbt"
)

def writeVersion(lineToChange: String, createLine: String => String, log: Logger, to: File*): Unit = {
  val newVersionLine = createLine(readTag)
  to foreach write(lineToChange, newVersionLine, log)
}

def write(lineToChange: String, newVersionLine: String, log: Logger)(file: File): Unit = {

  val fileLines = IO.readLines(file)

  val updatedLines = fileLines.map {
    case line if line startsWith lineToChange => newVersionLine
    case line                                 => line
  }

  IO.writeLines(file, updatedLines)

  log.info(s"'$newVersionLine' written to $file")
}

def readTag: String =
  Vcs
    .detect(root.base)
    .map(_.cmd("describe", "--tags").!!.trim)
    .getOrElse(sys.error("Release Tag cannot be checked"))
