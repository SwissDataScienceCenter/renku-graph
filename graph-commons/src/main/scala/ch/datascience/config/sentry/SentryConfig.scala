/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.config.sentry

import SentryConfig._
import cats.MonadError
import cats.implicits._
import ch.datascience.tinytypes.constraints.{NonBlank, Url, UrlOps}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}

import scala.language.higherKinds

final case class SentryConfig(baseUrl: SentryBaseUrl, environmentName: EnvironmentName, serviceName: ServiceName)

object SentryConfig {

  import ch.datascience.config.ConfigLoader._

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[Option[SentryConfig]] =
    find[Interpretation, Boolean]("services.sentry.enabled", config) flatMap {
      case false => ME.pure(None)
      case true =>
        for {
          url         <- find[Interpretation, SentryBaseUrl]("services.sentry.url", config)
          environment <- find[Interpretation, EnvironmentName]("services.sentry.environment-name", config)
          serviceName <- find[Interpretation, ServiceName]("services.sentry.service-name", config)
        } yield Some(SentryConfig(url, environment, serviceName))
    }

  class SentryBaseUrl private (val value: String) extends AnyVal with StringTinyType
  implicit object SentryBaseUrl
      extends TinyTypeFactory[SentryBaseUrl](new SentryBaseUrl(_))
      with Url
      with UrlOps[SentryBaseUrl]

  class ServiceName private (val value: String) extends AnyVal with StringTinyType
  implicit object ServiceName extends TinyTypeFactory[ServiceName](new ServiceName(_)) with NonBlank

  class EnvironmentName private (val value: String) extends AnyVal with StringTinyType
  implicit object EnvironmentName extends TinyTypeFactory[EnvironmentName](new EnvironmentName(_)) with NonBlank
}
