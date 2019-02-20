/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

organization := "ch.datascience"
name := "triples-generator"
version := "0.1.0-SNAPSHOT"

libraryDependencies += ws
libraryDependencies += "ch.datascience" %% "renku-commons" % "0.2.0"
libraryDependencies += "com.lihaoyi"    %% "ammonite-ops"  % "1.4.2"

val circeVersion = "0.10.0"
libraryDependencies += "io.circe" %% "circe-literal" % circeVersion
libraryDependencies += "io.circe" %% "circe-parser"  % circeVersion

libraryDependencies += "org.apache.jena" % "jena-rdfconnection" % "3.9.0"
libraryDependencies += "org.typelevel"   %% "cats-core"         % "1.4.0"

//Test dependencies
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
libraryDependencies += "org.scalamock"  %% "scalamock"  % "4.1.0"  % Test
libraryDependencies += "org.scalatest"  %% "scalatest"  % "3.0.5"  % Test

val jettyRequiredByWiremock = "9.2.24.v20180105"
dependencyOverrides += "org.eclipse.jetty" % "jetty-http"     % jettyRequiredByWiremock % Test
dependencyOverrides += "org.eclipse.jetty" % "jetty-io"       % jettyRequiredByWiremock % Test
dependencyOverrides += "org.eclipse.jetty" % "jetty-server"   % jettyRequiredByWiremock % Test
dependencyOverrides += "org.eclipse.jetty" % "jetty-servlet"  % jettyRequiredByWiremock % Test
dependencyOverrides += "org.eclipse.jetty" % "jetty-servlets" % jettyRequiredByWiremock % Test
dependencyOverrides += "org.eclipse.jetty" % "jetty-util"     % jettyRequiredByWiremock % Test
dependencyOverrides += "org.eclipse.jetty" % "jetty-webapp"   % jettyRequiredByWiremock % Test
