/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.db

import scala.sys.process._

class PostgresServer(module: String) {

  // When using a local postgres for development, use this env variable
  // to not start a postgres server via docker for the tests
  val skipServer: Boolean = sys.env.contains("NO_POSTGRES")

  val containerName = s"$module-test-postgres"
  val image         = "postgres:16.0-alpine"
  val startCmd =
    s"docker run --rm --name $containerName -e POSTGRES_PASSWORD=$module -e POSTGRES_USER=$module -e POSTGRES_DB=${module}_test -p 5432:5432 -d $image"
  val stopCmd    = s"docker stop -t5 $containerName"
  val isReadyCmd = s"docker exec $containerName pg_isready"

  def start(): Unit =
    if (skipServer) println(s"Not starting postgres via docker")
    else {
      println(s"Starting PostgreSQL container for module '$module' from '$image' image")
      startCmd.!!
      var rc = 1
      while (rc != 0) {
        Thread.sleep(500)
        rc = isReadyCmd.!
      }
    }

  def stop(): Any =
    if (!skipServer) {
      println(s"Stopping PostgreSQL container for module '$module'")
      stopCmd.!!
    }
}
