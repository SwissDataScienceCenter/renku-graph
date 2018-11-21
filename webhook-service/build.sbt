organization := "ch.datascience"
name := "webhook-service"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.7"

organizationName := "Swiss Data Science Center (SDSC)\nA partnership between École Polytechnique Fédérale de Lausanne (EPFL) and\nEidgenössische Technische Hochschule Zürich (ETHZ)."
startYear := Some(java.time.LocalDate.now().getYear)
licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

enablePlugins(PlayScala, JavaAppPackaging, AutomateHeaderPlugin)

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "ch.datascience" %% "renku-commons" % "0.2.0-SNAPSHOT"

libraryDependencies += "com.lihaoyi" %% "ammonite-ops" % "1.4.2"

libraryDependencies += "org.apache.jena" % "jena-rdfconnection" % "3.9.0"

libraryDependencies += "org.typelevel" %% "cats-core" % "1.4.0"

//Test dependencies
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

import scalariform.formatter.preferences._

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
