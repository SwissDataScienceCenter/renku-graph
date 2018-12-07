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

// Play JSON library
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.10"

val akkaStreamsVersion = "2.5.17"

// Akka Streams library
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaStreamsVersion

// ScalaTest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test

// Akka Streams test kit
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % akkaStreamsVersion % Test
