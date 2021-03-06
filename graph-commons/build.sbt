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

val pureConfigVersion = "0.16.0"
libraryDependencies += "com.github.pureconfig" %% "pureconfig"      % pureConfigVersion
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion

val refinedVersion = "0.9.26"
libraryDependencies += "eu.timepit" %% "refined"            % refinedVersion
libraryDependencies += "eu.timepit" %% "refined-pureconfig" % refinedVersion

val circeVersion = "0.14.1"
libraryDependencies += "io.circe" %% "circe-core"    % circeVersion
libraryDependencies += "io.circe" %% "circe-literal" % circeVersion
libraryDependencies += "io.circe" %% "circe-optics"  % circeVersion
libraryDependencies += "io.circe" %% "circe-parser"  % circeVersion

libraryDependencies += "io.sentry" % "sentry-logback" % "5.0.1"

val skunkVersion = "0.0.28"
libraryDependencies += "org.tpolecat" %% "skunk-core" % skunkVersion

val http4sVersion = "0.21.24"
libraryDependencies += "org.http4s" %% "http4s-blaze-client"       % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-blaze-server"       % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-circe"              % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-dsl"                % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-prometheus-metrics" % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-server"             % http4sVersion

val catsVersion = "2.6.0"
libraryDependencies += "org.typelevel" %% "cats-core"   % catsVersion
libraryDependencies += "org.typelevel" %% "cats-free"   % catsVersion
libraryDependencies += "org.typelevel" %% "cats-effect" % "2.4.1"

libraryDependencies += "org.typelevel" %% "log4cats-core" % "2.0.1"

// Test dependencies
val testContainersScalaVersion = "0.39.5"
libraryDependencies += "com.dimafeng" %% "testcontainers-scala-scalatest"  % testContainersScalaVersion % Test
libraryDependencies += "com.dimafeng" %% "testcontainers-scala-postgresql" % testContainersScalaVersion % Test

libraryDependencies += "com.github.tomakehurst" % "wiremock" % "2.27.2" % Test

val jenaVersion = "3.14.0" // 4.0.0 requires IRI encoding
libraryDependencies += "org.apache.jena" % "jena-fuseki-main"   % jenaVersion % Test
libraryDependencies += "org.apache.jena" % "jena-rdfconnection" % jenaVersion % Test
libraryDependencies += "org.apache.jena" % "jena-text"          % jenaVersion % Test

libraryDependencies += "org.scalacheck"    %% "scalacheck"      % "1.14.3"  % Test // version 1.15.1 is broken
libraryDependencies += "org.scalamock"     %% "scalamock"       % "5.1.0"   % Test
libraryDependencies += "org.scalatest"     %% "scalatest"       % "3.2.6"   % Test
libraryDependencies += "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % Test
