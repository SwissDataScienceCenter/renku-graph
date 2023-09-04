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
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.dimafeng.testcontainers.ForAllTestContainer
import io.renku.triplesstore.client.http.{ConnectionConfig, FusekiClient, SparqlClient}
import org.http4s.{BasicCredentials, Uri}
import org.scalatest.Suite
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait JenaContainerSupport extends ForAllTestContainer { self: Suite =>

  protected val runMode: JenaRunMode = JenaRunMode.GenericContainer
  protected val timeout: Duration    = 2.minutes

  lazy val container = JenaContainer.create(runMode)

  protected lazy val jenaUri: Uri = JenaContainer.fusekiUri(runMode, container)

  def clientResource(implicit L: Logger[IO]): Resource[IO, FusekiClient[IO]] = {
    val cc = ConnectionConfig(jenaUri, Some(BasicCredentials("admin", "admin")), None)
    FusekiClient[IO](cc, timeout)
  }

  def sparqlResource(datasetName: String)(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] =
    clientResource.map(_.sparql(datasetName))

  def withDataset(name: String)(implicit L: Logger[IO]): Resource[IO, SparqlClient[IO]] = {
    def datasetResource(c: FusekiClient[IO]) =
      Resource.make(c.createDataset(name, persistent = false))(_ => c.deleteDataset(name))

    clientResource.flatMap(c => datasetResource(c).as(c.sparql(name)))
  }
}
