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

import cats.MonadError
import cats.syntax.all._
import ch.datascience.config.sentry.SentryConfig.SentryBaseUrl

import scala.language.higherKinds
import scala.util.Try

class SentryInitializer[Interpretation[_]](
    maybeSentryConfig: Option[SentryConfig],
    initSentry:        String => Unit
)(implicit ME:         MonadError[Interpretation, Throwable]) {

  def run: Interpretation[Unit] =
    maybeSentryConfig.map(toDsn) match {
      case Some(dsn) => ME.fromTry(Try(initSentry(dsn.toString)))
      case _         => ME.unit
    }

  private lazy val toDsn: SentryConfig => SentryBaseUrl = { case SentryConfig(baseUrl, environmentName, serviceName) =>
    baseUrl ? ("stacktrace.app.packages" -> "") & ("servername" -> serviceName) & ("environment" -> environmentName)
  }
}

object SentryInitializer {
  import cats.MonadError
  import io.sentry.Sentry

  def apply[Interpretation[_]]()(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[SentryInitializer[Interpretation]] =
    for {
      maybeSentryConfig <- SentryConfig[Interpretation]()
    } yield new SentryInitializer(
      maybeSentryConfig,
      dns => { Sentry.init(dns); () }
    )
}
