/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
name := "triples-store-client"

Test / testOptions += Tests.Setup(jenaServer("start"))
Test / testOptions += Tests.Cleanup(jenaServer("forceStop"))

def jenaServer(methodName: String): ClassLoader => Unit = classLoader => {
  val clazz    = classLoader.loadClass("io.renku.triplesstore.client.util.TSClientJenaServer$")
  val method   = clazz.getMethod(methodName)
  val instance = clazz.getField("MODULE$").get(null)
  method.invoke(instance)
}

libraryDependencies ++=
  Dependencies.jsonld4s ++
    Dependencies.luceneQueryParser ++
    Dependencies.luceneAnalyzer ++
    Dependencies.fs2Core ++
    Dependencies.rdf4jQueryParserSparql ++
    Dependencies.http4sClient ++
    Dependencies.http4sCirce

libraryDependencies ++=
  (Dependencies.scalacheck ++
    Dependencies.scalatest ++
    Dependencies.catsEffectScalaTest ++
    Dependencies.catsEffectMunit ++
    Dependencies.scalacheckEffectMunit ++
    Dependencies.testContainersScalaTest ++
    Dependencies.scalatestScalaCheck).map(_ % Test)
