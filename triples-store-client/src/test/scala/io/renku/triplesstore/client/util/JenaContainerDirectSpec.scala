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

package io.renku.triplesstore.client.util

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.renku.triplesstore.client.http.{FusekiClient, SparqlClient}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.typelevel.log4cats.Logger

/** Trait for having a client directly accessible using "unsafe" effects. */
trait JenaContainerDirectSpec extends JenaContainerSpec with BeforeAndAfterAll { self: Suite =>
  implicit def logger: Logger[IO]

  implicit val ioRuntime: IORuntime

  private var tearDown:       IO[Unit]         = IO.unit
  protected var fusekiClient: FusekiClient[IO] = _

  protected def sparqlClient(datasetName: String): SparqlClient[IO] = fusekiClient.sparql(datasetName)

  override def afterStart(): Unit = {
    super.afterStart()
    val (client, shutdown) = clientResource.allocated.unsafeRunSync()
    this.tearDown = shutdown
    this.fusekiClient = client
  }

  override def afterAll(): Unit = {
    super.afterAll()
    tearDown.unsafeRunSync()
  }
}
