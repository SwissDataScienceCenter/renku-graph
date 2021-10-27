/*
 * Copyright 2018 - Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "graph-commons"

val pureConfigVersion = "0.17.0"
libraryDependencies += "com.github.pureconfig" %% "pureconfig"      % pureConfigVersion
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion

libraryDependencies += "eu.timepit"   %% "refined-pureconfig" % "0.9.27"
libraryDependencies += "io.sentry"     % "sentry-logback"     % "5.2.4"
libraryDependencies += "org.tpolecat" %% "skunk-core"         % "0.2.2"

val http4sVersion = "0.23.6"
libraryDependencies += "org.http4s" %% "http4s-blaze-client"       % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-blaze-server"       % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-circe"              % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-dsl"                % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-prometheus-metrics" % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-server"             % http4sVersion

libraryDependencies += "org.typelevel" %% "cats-effect"   % "3.2.9"
libraryDependencies += "org.typelevel" %% "log4cats-core" % "2.1.1"

// Test dependencies
val testContainersScalaVersion = "0.39.8"
libraryDependencies += "com.dimafeng"          %% "testcontainers-scala-scalatest"  % testContainersScalaVersion % Test
libraryDependencies += "com.dimafeng"          %% "testcontainers-scala-postgresql" % testContainersScalaVersion % Test
libraryDependencies += "com.github.tomakehurst" % "wiremock"                        % "2.27.2"                   % Test

val jenaVersion = "3.14.0" // 4.0.0 requires IRI encoding
libraryDependencies += "org.apache.jena" % "jena-fuseki-main"   % jenaVersion % Test
libraryDependencies += "org.apache.jena" % "jena-rdfconnection" % jenaVersion % Test
libraryDependencies += "org.apache.jena" % "jena-text"          % jenaVersion % Test

libraryDependencies += "org.scalamock" %% "scalamock" % "5.1.0" % Test
