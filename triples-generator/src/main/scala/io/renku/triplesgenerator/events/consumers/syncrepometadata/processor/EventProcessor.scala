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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.eventlog.api.EventLogClient.EventPayload
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[syncrepometadata] trait EventProcessor[F[_]] {
  def process(event: SyncRepoMetadata): F[Unit]
}

private[syncrepometadata] object EventProcessor {
  def apply[F[
      _
  ]: Async: NonEmptyParallel: Logger: AccessTokenFinder: GitLabClient: SparqlQueryTimeRecorder: MetricsRegistry](
      config: Config
  ): F[EventProcessor[F]] =
    (TSDataFinder[F](config), LatestPayloadFinder[F], UpdateCommandsRunner[F](config))
      .mapN(new EventProcessorImpl[F](_, GLDataFinder[F], _, PayloadDataExtractor[F], UpdateCommandsCalculator[F](), _))
}

private class EventProcessorImpl[F[_]: Async: NonEmptyParallel: Logger](
    tsDataFinder:             TSDataFinder[F],
    glDataFinder:             GLDataFinder[F],
    payloadFinder:            LatestPayloadFinder[F],
    payloadDataExtractor:     PayloadDataExtractor[F],
    updateCommandsCalculator: UpdateCommandsCalculator[F],
    updateCommandsRunner:     UpdateCommandsRunner[F]
) extends EventProcessor[F] {

  import glDataFinder.fetchGLData
  import payloadFinder.fetchLatestPayload
  import tsDataFinder.fetchTSData
  import updateCommandsCalculator.calculateUpdateCommands

  override def process(event: SyncRepoMetadata): F[Unit] =
    Logger[F].info(show"$categoryName: $event accepted") >>
      (fetchTSData(event.path), fetchGLData(event.path), fetchLatestPayload(event.path) >>= extractPayloadData(event))
        .parFlatMapN {
          case (Some(tsData), Some(glData), maybePayloadData) =>
            calculateUpdateCommands(tsData, glData, maybePayloadData) >>= executeUpdates
          case _ =>
            ().pure[F]
        }
        .handleErrorWith(logError(event))

  private def extractPayloadData(event: SyncRepoMetadata): Option[EventPayload] => F[Option[DataExtract.Payload]] = {
    case None          => Option.empty[DataExtract.Payload].pure[F]
    case Some(payload) => payloadDataExtractor.extractPayloadData(event.path, payload)
  }

  private def executeUpdates: List[UpdateCommand] => F[Unit] =
    _.traverse_(updateCommandsRunner.run)

  private def logError(event: SyncRepoMetadata): Throwable => F[Unit] = { exception =>
    Logger[F].error(exception)(show"$categoryName: $event processing failure")
  }
}
