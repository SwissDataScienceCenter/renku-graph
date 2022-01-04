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
import io.renku.config.sentry.SentryConfig.SentryBaseUrl
import io.sentry.SentryOptions

import scala.util.Try

trait SentryInitializer[F[_]] {
  def run(): F[Unit]
}

class SentryInitializerImpl[F[_]: MonadThrow](
    maybeSentryConfig: Option[SentryConfig],
    initSentry:        String => Unit
) extends SentryInitializer[F] {

  override def run(): F[Unit] = maybeSentryConfig.map(toDsn) match {
    case Some(dsn) => MonadThrow[F].fromTry(Try(initSentry(dsn.toString)))
    case _         => MonadThrow[F].unit
  }

  private lazy val toDsn: SentryConfig => SentryBaseUrl = {
    case SentryConfig(baseUrl, environmentName, serviceName, stackTracePackage) =>
      baseUrl ? ("stacktrace.app.packages" -> stackTracePackage) & ("servername" -> serviceName) & ("environment" -> environmentName)
  }
}

object SentryInitializer {
  import io.sentry.Sentry

  def apply[F[_]: MonadThrow]: F[SentryInitializer[F]] = for {
    maybeSentryConfig <- SentryConfig[F]()
  } yield new SentryInitializerImpl(
    maybeSentryConfig,
    dsn => { Sentry.init((options: SentryOptions) => options.setDsn(dsn)); () }
  )
}
