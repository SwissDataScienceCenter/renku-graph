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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.ApplicativeThrow
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.eventlog
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait UpdateCommandsRunner[F[_]] {
  def run(command: UpdateCommand): F[Unit]
}

private object UpdateCommandsRunner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry](config: Config): F[UpdateCommandsRunner[F]] =
    (ProjectsConnectionConfig[F](config).map(TSClient[F](_)), eventlog.api.events.Client[F](config))
      .mapN(new UpdateCommandsRunnerImpl(_, _))
}

private class UpdateCommandsRunnerImpl[F[_]: ApplicativeThrow: Logger](tsClient: TSClient[F],
                                                                       elClient: eventlog.api.events.Client[F]
) extends UpdateCommandsRunner[F] {

  override def run(command: UpdateCommand): F[Unit] = command match {
    case u: UpdateCommand.Sparql => executeQuery(u.value)
    case u: UpdateCommand.Event  => sendEvent(u.value)
  }

  private def executeQuery(query: SparqlQuery) =
    tsClient
      .updateWithNoResult(query)
      .handleErrorWith(Logger[F].error(_)(show"$categoryName: running '${query.name.value}' fails"))

  private def sendEvent(event: StatusChangeEvent.RedoProjectTransformation) =
    elClient
      .send(event)
      .handleErrorWith(Logger[F].error(_)(show"$categoryName: sending $event fails"))
}
