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

import cats.effect.std.Random
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.renku.triplesstore.client.http.{ConnectionConfig, DatasetDefinition, FusekiClient, SparqlClient}
import org.http4s.BasicCredentials
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait JenaSpec extends BeforeAndAfterAll {
  self: Suite =>

  def server:            JenaServer
  protected val timeout: Duration = 2.minutes

  def clientResource(implicit L: Logger[IO]): Resource[IO, FusekiClient[IO]] = {
    val cc = ConnectionConfig(server.ccConfig.baseUrl, Some(BasicCredentials("admin", "admin")), retry = None)
    FusekiClient[IO](cc, timeout)
  }

  def testDSResource(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    Random
      .scalaUtilRandom[IO]
      .flatMap(_.nextIntBounded(1000))
      .map(v => s"${getClass.getSimpleName.toLowerCase}_$v")
      .toResource
      .flatMap(withDS(_))

  def withDS(name: String)(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    withDS(DatasetDefinition.inMemory(name))

  def withDS(dsDefinition: DatasetDefinition)(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    clientResource.flatMap(c => datasetResource(dsDefinition)(c).as(c.sparql(dsDefinition.name)))

  def datasetResource(dsDefinition: DatasetDefinition)(c: FusekiClient[IO]) =
    Resource.make(c.createDataset(dsDefinition))(_ => c.deleteDataset(dsDefinition.name))

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }

  protected override def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
