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

package io.renku.triplesstore.client.util

import cats.syntax.all._
import io.renku.triplesstore.client.http.ConnectionConfig
import org.http4s.{BasicCredentials, Uri}

import scala.sys.process._

class JenaServer(module: String, port: Int) {

  val conConfig: ConnectionConfig = ConnectionConfig(
    Uri.unsafeFromString(s"http://localhost:$port"),
    BasicCredentials(username = "admin", password = "admin").some,
    retry = None
  )

  // When using a local Jena for development, use this env variable
  // to not start a Jena server via docker for the tests
  private val skipServer: Boolean = sys.env.contains("NO_JENA")

  private val containerName = s"$module-test-jena"
  private val imageVersion  = "0.0.23"
  private val image         = s"renku/renku-jena:$imageVersion"
  private val startCmd = s"""|docker run --rm
                             |--name $containerName
                             |-p $port:3030
                             |-d $image""".stripMargin
  private val isRunningCmd = s"docker container ls --filter 'name=$containerName'"
  private val stopCmd      = s"docker stop -t5 $containerName"
  private val readyCmd     = "curl http://localhost:3030/$/ping --no-progress-meter --fail 1> /dev/null"
  private val isReadyCmd   = s"docker exec $containerName sh -c '$readyCmd'"
  private var wasRunning: Boolean = false

  def start(): Unit =
    if (skipServer) println("Not starting Jena via docker")
    else if (checkRunning) ()
    else {
      println(s"Starting Jena container for module '$module' from '$image' image")
      startCmd.!!
      var rc = 1
      while (rc != 0) {
        Thread.sleep(500)
        rc = isReadyCmd.!
        if (rc == 0) println(s"Jena container for module '$module' started on port $port")
      }
    }

  private def checkRunning: Boolean = {
    val out = isRunningCmd.lazyLines.toList
    wasRunning = out.exists(_ contains containerName)
    wasRunning
  }

  def stop(): Any =
    if (!skipServer && !wasRunning) {
      println(s"Stopping Jena container for module '$module'")
      stopCmd.!!
    }

  def forceStop(): Any =
    if (!skipServer) {
      println(s"Stopping Jena container for module '$module'")
      stopCmd.!!
    }
}