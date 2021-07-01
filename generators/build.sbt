/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

organization := "io.renku"
name := "generators"
scalaVersion := "2.13.6"

val refinedVersion = "0.9.26"
libraryDependencies += "eu.timepit" %% "refined" % refinedVersion

val circeVersion = "0.14.1"
libraryDependencies += "io.circe" %% "circe-core" % circeVersion

val catsVersion = "2.6.0"
libraryDependencies += "org.typelevel" %% "cats-core" % catsVersion

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.3" // version 1.15.1 is broken
