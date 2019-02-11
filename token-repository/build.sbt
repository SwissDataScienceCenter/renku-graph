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
name := "token-repository"
version := "0.1.0-SNAPSHOT"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

val doobieVersion = "0.6.0"
libraryDependencies += "org.tpolecat" %% "doobie-core"     % doobieVersion
libraryDependencies += "org.tpolecat" %% "doobie-postgres" % doobieVersion

//Test dependencies
libraryDependencies += "org.tpolecat"   %% "doobie-scalatest" % doobieVersion % Test
libraryDependencies += "org.scalacheck" %% "scalacheck"       % "1.14.0"      % Test
libraryDependencies += "org.scalamock"  %% "scalamock"        % "4.1.0"       % Test
libraryDependencies += "org.scalatest"  %% "scalatest"        % "3.0.5"       % Test
