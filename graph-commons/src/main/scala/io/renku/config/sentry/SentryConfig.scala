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

package io.renku.config.sentry

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.sentry.SentryConfig._
import io.renku.config.{ServiceName, ServiceVersion}
import io.renku.tinytypes.constraints.{NonBlank, Url, UrlOps}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}

final case class SentryConfig(baseUrl:         Dsn,
                              environmentName: Environment,
                              serviceName:     ServiceName,
                              serviceVersion:  ServiceVersion
)

object SentryConfig {

  import io.renku.config.ConfigLoader._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load(),
                              maybeVersionConfig: Option[Config] = None
  ): F[Option[SentryConfig]] = find[F, Boolean]("services.sentry.enabled", config) >>= {
    case false => Option.empty[SentryConfig].pure[F]
    case true =>
      for {
        url         <- find[F, Dsn]("services.sentry.dsn", config)
        environment <- find[F, Environment]("services.sentry.environment", config)
        serviceName <- ServiceName.readFromConfig(config)
        serviceVersion <-
          maybeVersionConfig.map(ServiceVersion.readFromConfig[F]).getOrElse(ServiceVersion.readFromConfig())
      } yield SentryConfig(url, environment, serviceName, serviceVersion).some
  }

  class Dsn private (val value: String) extends AnyVal with UrlTinyType
  implicit object Dsn                   extends TinyTypeFactory[Dsn](new Dsn(_)) with Url with UrlOps[Dsn]

  class Environment private (val value: String) extends AnyVal with StringTinyType
  implicit object Environment                   extends TinyTypeFactory[Environment](new Environment(_)) with NonBlank
}
