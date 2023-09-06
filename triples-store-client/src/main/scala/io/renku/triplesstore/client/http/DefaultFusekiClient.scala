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

package io.renku.triplesstore.client.http

import cats.effect._
import cats.syntax.all._
import org.http4s.Method.{DELETE, GET, POST}
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Accept
import org.http4s.{MediaType, Status, UrlForm}
import org.typelevel.log4cats.Logger

final class DefaultFusekiClient[F[_]: Async: Logger](
    client: Client[F],
    cc:     ConnectionConfig
) extends FusekiClient[F]
    with Http4sClientDsl[F]
    with MoreClientDsl[F] {

  private[this] val retry       = cc.retry.map(Retry.apply[F])
  private[this] val datasetsUri = cc.baseUrl / "$" / "datasets"

  override def sparql(datasetName: String): SparqlClient[F] = {
    val subConfig = cc.copy(baseUrl = cc.baseUrl / datasetName)
    new DefaultSparqlClient[F](client, subConfig)
  }

  override def datasetExists(name: String): F[Boolean] = {
    val req = GET(datasetsUri).withBasicAuth(cc.basicAuth)
    client.run(req).use { resp =>
      if (resp.status.isSuccess) true.pure[F]
      else if (resp.status == Status.NotFound) false.pure[F]
      else SparqlRequestError(s"getDataset($name)", resp).flatMap(Async[F].raiseError)
    }
  }

  override def createDataset(name: String, persistent: Boolean): F[Unit] =
    retry.fold(createDataset0(name, persistent))(_.retryConnectionError(createDataset0(name, persistent)))

  private def createDataset0(name: String, persistent: Boolean): F[Unit] = {
    val dbType = if (persistent) "tdb" else "mem"
    val req =
      POST(datasetsUri)
        .putHeaders(Accept(MediaType.application.json))
        .withBasicAuth(cc.basicAuth)
        .withEntity(UrlForm("dbType" -> dbType, "dbName" -> name))

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(s"createDataset($name)", resp).flatMap(Async[F].raiseError)
    }
  }

  override def createDatasetIfNotExists(name: String, persistent: Boolean): F[Unit] =
    datasetExists(name).flatMap {
      case true  => ().pure[F]
      case false => createDataset(name, persistent)
    }

  override def deleteDataset(name: String): F[Unit] =
    retry.fold(deleteDataset0(name))(_.retryConnectionError(deleteDataset0(name)))

  override def deleteDatasetIfExists(name: String): F[Unit] =
    datasetExists(name).flatMap {
      case true  => deleteDataset(name)
      case false => ().pure[F]
    }

  private def deleteDataset0(name: String): F[Unit] = {
    val req = DELETE(datasetsUri / name).withBasicAuth(cc.basicAuth)
    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(s"deleteDataset($name)", resp).flatMap(Async[F].raiseError)
    }
  }
}
