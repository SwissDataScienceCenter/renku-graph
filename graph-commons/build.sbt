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

organization := "ch.datascience"
name := "renku-graph-commons"
version := "0.1.0-SNAPSHOT"

val pureConfigVersion = "0.10.1"
libraryDependencies += "com.github.pureconfig" %% "pureconfig"      % pureConfigVersion
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion

val refinedVersion = "0.9.3"
libraryDependencies += "eu.timepit" %% "refined"            % refinedVersion
libraryDependencies += "eu.timepit" %% "refined-pureconfig" % refinedVersion

libraryDependencies += "io.chrisdavenport" %% "log4cats-core" % "0.2.0"

val circeVersion = "0.10.0"
libraryDependencies += "io.circe" %% "circe-core"    % circeVersion
libraryDependencies += "io.circe" %% "circe-literal" % circeVersion
libraryDependencies += "io.circe" %% "circe-parser"  % circeVersion

libraryDependencies += "org.typelevel" %% "cats-core" % "1.5.0"

val http4sVersion = "0.19.0"
libraryDependencies += "org.http4s" %% "http4s-blaze-client" % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-blaze-server" % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-circe"        % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-dsl"          % http4sVersion

// Test dependencies
libraryDependencies += "com.github.tomakehurst" % "wiremock"    % "2.18.0" % Test
libraryDependencies += "org.scalamock"          %% "scalamock"  % "4.1.0"  % Test
libraryDependencies += "org.scalacheck"         %% "scalacheck" % "1.14.0" % Test
libraryDependencies += "org.scalatest"          %% "scalatest"  % "3.0.5"  % Test
