organization := "ch.datascience"
name := "renku-graph"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.12.8"

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
  scalaVersion := "2.12.8",

  scalacOptions += "-Ypartial-unification",
    
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
  )),
  
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

