/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityEncoder, Response, Status}
import pureconfig.ConfigReader

private trait Endpoint[F[_]] {
  def `GET /version`: F[Response[F]]
}

private object Endpoint {
  def apply[F[_]: Async]: F[Endpoint[F]] = new EndpointImpl[F].pure[F].widen[Endpoint[F]]
}

private class EndpointImpl[F[_]: Async](config: Config = ConfigFactory.load(),
                                        versionConfig: Config = ConfigFactory.load("version.conf")
) extends Http4sDsl[F]
    with Endpoint[F] {
  import ConfigLoader._
  import io.circe.literal._
  import io.circe.syntax._
  import io.circe.{Encoder, Json}
  import org.http4s.circe.jsonEncoderOf

  private type ServiceInfo = (ServiceName, ServiceVersion)

  override def `GET /version`: F[Response[F]] = for {
    serviceInfo <- readConfigs
    response    <- Response[F](Status.Ok).withEntity(serviceInfo.asJson).pure[F]
  } yield response

  private def readConfigs: F[ServiceInfo] = for {
    serviceName    <- find[F, ServiceName]("service-name", config)
    serviceVersion <- find[F, ServiceVersion]("version", versionConfig)
  } yield serviceName -> serviceVersion

  private implicit lazy val encoder: Encoder[ServiceInfo] = Encoder { case (serviceName, serviceVersion) =>
    json"""{
      "name": ${serviceName.value},
      "versions": [
        {
          "version": ${serviceVersion.value}
        }
      ]
    }"""
  }

  private implicit lazy val responseEntityEncoder: EntityEncoder[F, Json] = jsonEncoderOf[F, Json]
}

final class ServiceName private (val value: String) extends AnyVal with StringTinyType
object ServiceName extends TinyTypeFactory[ServiceName](new ServiceName(_)) with NonBlank {
  import ConfigLoader._
  implicit val reader: ConfigReader[ServiceName] = stringTinyTypeReader(ServiceName)
}

final class ServiceVersion private (val value: String) extends AnyVal with StringTinyType
object ServiceVersion extends TinyTypeFactory[ServiceVersion](new ServiceVersion(_)) with NonBlank {
  import ConfigLoader._
  implicit val reader: ConfigReader[ServiceVersion] = stringTinyTypeReader(ServiceVersion)
}
