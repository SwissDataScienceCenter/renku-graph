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

package io.renku.triplesstore.client.http

import cats.effect._
import fs2.io.net.Network
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

/** A SPARQL client with additional features for Apache Jena Fuseki servers. */
trait FusekiClient[F[_]] {

  def createDataset(name: String, persistent: Boolean): F[Unit]

  def createDataset(definition: DatasetDefinition): F[Unit]

  def deleteDataset(name: String): F[Unit]

  def datasetExists(name: String): F[Boolean]

  def createDatasetIfNotExists(name: String, persistent: Boolean): F[Unit]

  def deleteDatasetIfExists(name: String): F[Unit]

  def sparql(datasetName: String): SparqlClient[F]
}

object FusekiClient {
  def apply[F[_]: Async: Network: Logger](
      connectionConfig: ConnectionConfig,
      timeout:          Duration = 20.minutes
  ): Resource[F, FusekiClient[F]] =
    EmberClientBuilder
      .default[F]
      .withTimeout(timeout)
      .withIdleConnectionTime(timeout * 1.1)
      .build
      .map(new DefaultFusekiClient[F](_, connectionConfig))
}
