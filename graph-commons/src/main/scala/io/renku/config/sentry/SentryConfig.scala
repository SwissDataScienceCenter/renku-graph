/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import io.renku.tinytypes.constraints.{NonBlank, Url, UrlOps}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}

import scala.util.control.NonFatal

final case class SentryConfig(baseUrl:           SentryBaseUrl,
                              environmentName:   EnvironmentName,
                              serviceName:       ServiceName,
                              stackTracePackage: SentryStackTracePackage
)

object SentryConfig {

  import io.renku.config.ConfigLoader._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[Option[SentryConfig]] = {
    lazy val emptyString: PartialFunction[Throwable, F[SentryStackTracePackage]] = { case NonFatal(_) =>
      SentryStackTracePackage.empty.pure[F]
    }

    find[F, Boolean]("services.sentry.enabled", config) flatMap {
      case false => MonadThrow[F].pure(None)
      case true =>
        for {
          url         <- find[F, SentryBaseUrl]("services.sentry.url", config)
          environment <- find[F, EnvironmentName]("services.sentry.environment-name", config)
          serviceName <- find[F, ServiceName]("services.sentry.service-name", config)
          stackTracePackage <-
            find[F, SentryStackTracePackage]("services.sentry.stacktrace-package", config) recoverWith emptyString
        } yield Some(
          SentryConfig(url, environment, serviceName, stackTracePackage)
        )
    }
  }

  class SentryBaseUrl private (val value: String) extends AnyVal with UrlTinyType
  implicit object SentryBaseUrl
      extends TinyTypeFactory[SentryBaseUrl](new SentryBaseUrl(_))
      with Url
      with UrlOps[SentryBaseUrl]

  class ServiceName private (val value: String) extends AnyVal with StringTinyType
  implicit object ServiceName                   extends TinyTypeFactory[ServiceName](new ServiceName(_)) with NonBlank

  class SentryStackTracePackage private (val value: String) extends AnyVal with StringTinyType
  implicit object SentryStackTracePackage
      extends TinyTypeFactory[SentryStackTracePackage](new SentryStackTracePackage(_)) {
    lazy val empty: SentryStackTracePackage = SentryStackTracePackage("")
  }

  class EnvironmentName private (val value: String) extends AnyVal with StringTinyType
  implicit object EnvironmentName extends TinyTypeFactory[EnvironmentName](new EnvironmentName(_)) with NonBlank
}
