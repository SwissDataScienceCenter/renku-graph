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
version := "0.1.0-SNAPSHOT"
name := "renku-graph-commons"

val pureConfigVersion = "0.10.1"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
val playVersion = "2.6.10"
libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion
val refinedVersion = "0.9.3"
libraryDependencies += "eu.timepit" %% "refined" % refinedVersion
libraryDependencies += "eu.timepit" %% "refined-pureconfig" % refinedVersion
val log4CatsVersion = "0.2.0"
libraryDependencies += "io.chrisdavenport" %% "log4cats-core" % log4CatsVersion
libraryDependencies += "io.chrisdavenport" %% "log4cats-extras" % log4CatsVersion
libraryDependencies += "io.chrisdavenport" %% "log4cats-slf4j" % log4CatsVersion
libraryDependencies += "org.typelevel" %% "cats-core" % "1.5.0"

// Test dependencies
libraryDependencies += "com.typesafe.play" %% "play-test" % playVersion
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
