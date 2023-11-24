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

Test / testOptions += Tests.Setup(postgresServer("start"))
Test / testOptions += Tests.Cleanup(postgresServer("stop"))

def postgresServer(methodName: String): ClassLoader => Unit = classLoader => {
  val clazz    = classLoader.loadClass("io.renku.db.CommonsPostgresServer$")
  val method   = clazz.getMethod(methodName)
  val instance = clazz.getField("MODULE$").get(null)
  method.invoke(instance)
}

libraryDependencies ++=
  Dependencies.pureconfig ++
    Dependencies.refinedPureconfig ++
    Dependencies.sentryLogback ++
    Dependencies.luceneQueryParser ++
    Dependencies.http4sClient ++
    Dependencies.http4sServer ++
    Dependencies.http4sCirce ++
    Dependencies.http4sDsl ++
    Dependencies.http4sPrometheus ++
    Dependencies.skunk ++
    Dependencies.catsEffect ++
    Dependencies.log4Cats

// Test dependencies
libraryDependencies ++=
  (Dependencies.testContainersPostgres ++
    Dependencies.wiremock ++
    Dependencies.scalamock).map(_ % Test)
