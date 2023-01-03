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

Test / fork := true

val pureConfigVersion = "0.17.2"
libraryDependencies += "com.github.pureconfig" %% "pureconfig"      % pureConfigVersion
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion

libraryDependencies += "eu.timepit"       %% "refined-pureconfig" % "0.10.1"
libraryDependencies += "io.sentry"         % "sentry-logback"     % "6.11.0"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "9.4.2"

val http4sVersion           = "0.23.16"
val http4sBlazeVersion      = "0.23.13"
val http4sPrometheusVersion = "0.24.2"
libraryDependencies += "org.http4s" %% "http4s-blaze-client"       % http4sBlazeVersion
libraryDependencies += "org.http4s" %% "http4s-blaze-server"       % http4sBlazeVersion
libraryDependencies += "org.http4s" %% "http4s-circe"              % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-dsl"                % http4sVersion
libraryDependencies += "org.http4s" %% "http4s-prometheus-metrics" % http4sPrometheusVersion
libraryDependencies += "org.http4s" %% "http4s-server"             % http4sVersion

libraryDependencies += "org.tpolecat"  %% "skunk-core"    % "0.3.2"
libraryDependencies += "org.typelevel" %% "cats-effect"   % "3.4.4"
libraryDependencies += "org.typelevel" %% "log4cats-core" % "2.5.0"

// Test dependencies
val testContainersScalaVersion = "0.40.12"
libraryDependencies += "com.dimafeng"          %% "testcontainers-scala-scalatest"  % testContainersScalaVersion % Test
libraryDependencies += "com.dimafeng"          %% "testcontainers-scala-postgresql" % testContainersScalaVersion % Test
libraryDependencies += "com.github.tomakehurst" % "wiremock-jre8"                   % "2.35.0"                   % Test

libraryDependencies += "org.scalamock" %% "scalamock" % "5.2.0" % Test
