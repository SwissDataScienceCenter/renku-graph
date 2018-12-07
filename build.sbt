organization := "ch.datascience"
name := "renku-graph"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.12.7"

// This project contains nothing to package, like pure POM maven project
packagedArtifacts := Map.empty

lazy val root = Project(
  id   = "renku-graph",
  base = file(".")
).aggregate(
  graphCommons, 
  webhookService,
  triplesGenerator
)

lazy val graphCommons = Project(
  id   = "graph-commons",
  base = file("graph-commons")
).settings(
  libraryDependencies += playCore,
  commonSettings
).enablePlugins(
  AutomateHeaderPlugin
)

lazy val webhookService = Project(
  id   = "webhook-service",
  base = file("webhook-service")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  PlayScala, 
  JavaAppPackaging, 
  AutomateHeaderPlugin
)

lazy val triplesGenerator = Project(
  id   = "triples-generator",
  base = file("triples-generator")
).settings(
  commonSettings
).dependsOn(
  graphCommons % "compile->compile",
  graphCommons % "test->test"
).enablePlugins(
  PlayScala, 
  JavaAppPackaging, 
  AutomateHeaderPlugin
)

import scalariform.formatter.preferences._

lazy val commonSettings = Seq(
  organization := "ch.datascience",
  scalaVersion := "2.12.7",

  organizationName := "Swiss Data Science Center (SDSC)\nA partnership between École Polytechnique Fédérale de Lausanne (EPFL) and\nEidgenössische Technische Hochschule Zürich (ETHZ).",
  startYear := Some(java.time.LocalDate.now().getYear),
  licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),

  scalariformPreferences := scalariformPreferences.value
    .setPreference( AlignArguments,                               true  )
    .setPreference( AlignParameters,                              true  )
    .setPreference( AlignSingleLineCaseStatements,                true  )
    .setPreference( AlignSingleLineCaseStatements.MaxArrowIndent, 40    )
    .setPreference( CompactControlReadability,                    true  )
    .setPreference( CompactStringConcatenation,                   false )
    .setPreference( DanglingCloseParenthesis,                     Force )
    .setPreference( DoubleIndentConstructorArguments,             true  )
    .setPreference( DoubleIndentMethodDeclaration,                true  )
    .setPreference( FirstArgumentOnNewline,                       Force )
    .setPreference( FirstParameterOnNewline,                      Force )
    .setPreference( FormatXml,                                    true  )
    .setPreference( IndentPackageBlocks,                          true  )
    .setPreference( IndentSpaces,                                 2     )
    .setPreference( IndentWithTabs,                               false )
    .setPreference( MultilineScaladocCommentsStartOnFirstLine,    false )
    .setPreference( NewlineAtEndOfFile,                           true  )
    .setPreference( PlaceScaladocAsterisksBeneathSecondAsterisk,  false )
    .setPreference( PreserveSpaceBeforeArguments,                 false )
    .setPreference( RewriteArrowSymbols,                          false )
    .setPreference( SpaceBeforeColon,                             false )
    .setPreference( SpaceBeforeContextColon,                      true  )
    .setPreference( SpaceInsideBrackets,                          false )
    .setPreference( SpaceInsideParentheses,                       true  )
    .setPreference( SpacesAroundMultiImports,                     true  )
    .setPreference( SpacesWithinPatternBinders,                   false )
)

