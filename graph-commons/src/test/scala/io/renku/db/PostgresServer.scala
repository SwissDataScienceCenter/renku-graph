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

package io.renku.db

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig

import scala.sys.process._

class PostgresServer(module: String, port: Int) {

  val dbConfig: DBConfigProvider.DBConfig[PostgresServer] = DBConfig[PostgresServer](
    name = Refined.unsafeApply(s"${module}_test"),
    host = "localhost",
    port = Refined.unsafeApply(port),
    user = Refined.unsafeApply(module),
    pass = Refined.unsafeApply(module),
    connectionPool = 1
  )

  // When using a local postgres for development, use this env variable
  // to not start a postgres server via docker for the tests
  private val skipServer: Boolean = sys.env.contains("NO_POSTGRES")

  private val containerName = s"$module-test-postgres"
  private val image         = "postgres:16.0-alpine"
  private val startCmd = s"""|docker run --rm
                             |--name $containerName
                             |-e POSTGRES_USER=${dbConfig.user}
                             |-e POSTGRES_PASSWORD=${dbConfig.pass}
                             |-e POSTGRES_DB=${dbConfig.name}
                             |-p ${dbConfig.port}:5432
                             |-d $image""".stripMargin
  private val isRunningCmd = s"docker container ls --filter 'name=$containerName'"
  private val stopCmd      = s"docker stop -t5 $containerName"
  private val isReadyCmd   = s"docker exec $containerName pg_isready"
  private var wasRunning: Boolean = false
  private var sbtStarted: Boolean = false

  def sbtStart(): Unit = {
    start()
    sbtStarted = true
  }

  def start(): Unit =
    if (skipServer) println("Not starting postgres via docker")
    else if (checkRunning) ()
    else {
      println(s"Starting PostgreSQL container for module '$module' from '$image' image")
      startCmd.!!
      var rc = 1
      while (rc != 0) {
        Thread.sleep(500)
        rc = isReadyCmd.!
        if (rc == 0) println(s"PostgreSQL container for module '$module' started on port $port")
      }
    }

  private def checkRunning: Boolean = {
    val out = isRunningCmd.lazyLines.toList
    wasRunning = out.exists(_ contains containerName)
    wasRunning
  }

  def stop(): Unit =
    if (!skipServer && !wasRunning && !sbtStarted) {
      println(s"Stopping PostgreSQL container for module '$module'")
      stopCmd.!!
      ()
    }

  def forceStop(): Unit =
    if (!skipServer) {
      println(s"Stopping PostgreSQL container for module '$module'")
      stopCmd.!!
      ()
    }
}
