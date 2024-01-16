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

package io.renku.http.server.version

import cats.effect.Async
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.config.{ServiceName, ServiceVersion}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Response, Status}

trait Endpoint[F[_]] {
  def `GET /version`: F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async]: F[Endpoint[F]] = for {
    serviceName    <- ServiceName.readFromConfig[F]()
    serviceVersion <- ServiceVersion.readFromConfig[F]()
  } yield new EndpointImpl[F](serviceName, serviceVersion)

  type ServiceInfo = (ServiceName, ServiceVersion)

  implicit lazy val encoder: Encoder[ServiceInfo] = Encoder { case (serviceName, serviceVersion) =>
    json"""{
      "name": ${serviceName.value},
      "versions": [
        {
          "version": ${serviceVersion.value}
        }
      ]
    }"""
  }
}

private class EndpointImpl[F[_]: Async](serviceName: ServiceName, serviceVersion: ServiceVersion)
    extends Http4sDsl[F]
    with Endpoint[F] {

  import Endpoint.encoder

  override def `GET /version`: F[Response[F]] =
    Response[F](Status.Ok)
      .withEntity((serviceName -> serviceVersion).asJson)
      .pure[F]
}
