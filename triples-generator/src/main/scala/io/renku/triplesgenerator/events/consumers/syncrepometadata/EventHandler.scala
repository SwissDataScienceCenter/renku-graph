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

import cats.NonEmptyParallel
import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import com.typesafe.config.Config
import eu.timepit.refined.auto._
import io.renku.events.consumers.EventDecodingTools._
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.{CategoryName, consumers}
import io.renku.http.client.GitLabClient
import io.renku.lock.syntax._
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.TgDB.TsWriteLock
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import processor.EventProcessor

private[syncrepometadata] class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    tsReadinessChecker:        TSReadinessForEventsChecker[F],
    eventProcessor:            EventProcessor[F],
    processExecutor:           ProcessExecutor[F],
    tsWriteLock:               TsWriteLock[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = SyncRepoMetadata

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      _.event.getProjectSlug.map(SyncRepoMetadata(_)),
      tsWriteLock.contramap[Event](_.slug).surround(eventProcessor.process),
      precondition = tsReadinessChecker.verifyTSReady
    )
}

private object EventHandler {
  def apply[F[
      _
  ]: Async: NonEmptyParallel: GitLabClient: Logger: ReProvisioningStatus: SparqlQueryTimeRecorder: MetricsRegistry](
      config:      Config,
      tsWriteLock: TsWriteLock[F]
  ): F[consumers.EventHandler[F]] = for {
    tsReadinessChecker <- TSReadinessForEventsChecker[F](config)
    eventProcessor     <- EventProcessor[F](config)
    processExecutor    <- ProcessExecutor.concurrent(1)
  } yield new EventHandler[F](categoryName, tsReadinessChecker, eventProcessor, processExecutor, tsWriteLock)
}
