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

val pureConfigVersion = "0.13.0"
libraryDependencies += "com.github.pureconfig" %% "pureconfig"      % pureConfigVersion
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion

val refinedVersion = "0.9.15"
libraryDependencies += "eu.timepit" %% "refined"            % refinedVersion
libraryDependencies += "eu.timepit" %% "refined-pureconfig" % refinedVersion

libraryDependencies += "io.chrisdavenport" %% "log4cats-core" % "1.1.1"

val circeVersion = "0.13.0"
libraryDependencies += "io.circe" %% "circe-core"    % circeVersion
libraryDependencies += "io.circe" %% "circe-literal" % circeVersion
libraryDependencies += "io.circe" %% "circe-parser"  % circeVersion
libraryDependencies += "io.circe" %% "circe-optics"  % circeVersion

libraryDependencies += "io.sentry" % "sentry-logback" % "1.7.30"

val doobieVersion = "0.8.8"
libraryDependencies += "org.tpolecat" %% "doobie-core"     % doobieVersion
libraryDependencies += "org.tpolecat" %% "doobie-hikari"   % doobieVersion
libraryDependencies += "org.tpolecat" %% "doobie-postgres" % doobieVersion

libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"

val http4sVersion = "0.21.6"
libraryDependencies += "org.http4s" %% "http4s-blaze-client"       % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-blaze-server"       % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-server"             % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-circe"              % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-dsl"                % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-prometheus-metrics" % http4sVersion

// Test dependencies
libraryDependencies += "com.github.tomakehurst" % "wiremock" % "2.27.1" % Test

val jenaVersion = "3.16.0"
libraryDependencies += "org.apache.jena" % "jena-fuseki-main"   % jenaVersion % Test
libraryDependencies += "org.apache.jena" % "jena-rdfconnection" % jenaVersion % Test
libraryDependencies += "org.apache.jena" % "jena-text"          % jenaVersion % Test

libraryDependencies += "org.scalamock"     %% "scalamock"       % "5.0.0"   % Test
libraryDependencies += "org.scalacheck"    %% "scalacheck"      % "1.14.3"  % Test
libraryDependencies += "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % Test
libraryDependencies += "org.scalatest"     %% "scalatest"       % "3.2.1"   % Test

libraryDependencies += "org.tpolecat" %% "doobie-h2"        % doobieVersion % Test
libraryDependencies += "org.tpolecat" %% "doobie-scalatest" % doobieVersion % Test
