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
import cats.syntax.all._
import io.circe.Json
import io.renku.jsonld.JsonLD
import org.http4s.MediaType
import org.http4s.MediaType.application.`ld+json`
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{Accept, `Content-Type`}
import org.http4s.implicits._
import org.typelevel.log4cats.Logger

final class DefaultSparqlClient[F[_]: Async: Logger](client: Client[F], config: ConnectionConfig)
    extends SparqlClient[F]
    with Http4sClientDsl[F]
    with MoreClientDsl[F] {

  private[this] val retry = config.retry.map(Retry.apply[F])
  private[this] val sparqlResultsJson: MediaType = mediaType"application/sparql-results+json"

  override def update(request: SparqlUpdate): F[Unit] =
    retry.fold(update0(request))(_.retryConnectionError(update0(request)))

  private def update0(request: SparqlUpdate): F[Unit] = {
    val req =
      POST(config.baseUrl / "update")
        .putHeaders(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
        .withEntity(request)

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(request.render, resp).flatMap(Async[F].raiseError)
    }
  }

  override def upload(data: JsonLD): F[Unit] =
    retry.fold(upload0(data))(_.retryConnectionError(upload0(data)))

  private def upload0(data: JsonLD): F[Unit] = {
    val req =
      POST(config.baseUrl / "data")
        .putHeaders(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
        .withEntity(data.toJson)
        .withContentType(`Content-Type`(`ld+json`))

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(data.toJson.noSpaces, resp).flatMap(Async[F].raiseError)
    }
  }

  override def query(request: SparqlQuery): F[Json] =
    retry.fold(query0(request))(_.retryConnectionError(query0(request)))

  private def query0(request: SparqlQuery): F[Json] = {
    val req =
      POST(config.baseUrl / "query")
        .addHeader(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
        .withEntity(request)

    client.run(req).use { resp =>
      if (resp.status.isSuccess) resp.as[Json]
      else SparqlRequestError(request.render, resp).flatMap(Async[F].raiseError)
    }
  }
}
