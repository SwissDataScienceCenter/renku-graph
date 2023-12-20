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

package io.renku.commiteventservice.events.consumers.globalcommitsync

import cats.NonEmptyParallel
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.commiteventservice.events.consumers.globalcommitsync.GlobalCommitSyncEvent.CommitsInfo
import io.renku.commiteventservice.events.consumers.globalcommitsync.eventgeneration.CommitsSynchronizer
import io.renku.events.consumers._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.{CategoryName, consumers}
import io.renku.graph.model.events.CommitId
import io.renku.http.client.GitLabClient
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    commitEventSynchronizer:   CommitsSynchronizer[F],
    subscriptionMechanism:     SubscriptionMechanism[F],
    processExecutor:           ProcessExecutor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = GlobalCommitSyncEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[GlobalCommitSyncEvent],
      process = commitEventSynchronizer.synchronizeEvents,
      onRelease = subscriptionMechanism.renewSubscription()
    )

  import EventDecodingTools._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  private implicit val eventDecoder: Decoder[GlobalCommitSyncEvent] = cursor =>
    for {
      project        <- cursor.value.getProject
      commitsCount   <- cursor.downField("commits").downField("count").as[CommitsCount]
      latestCommitId <- cursor.downField("commits").downField("latest").as[CommitId]
    } yield GlobalCommitSyncEvent(project, CommitsInfo(commitsCount, latestCommitId))
}

private object EventHandler {

  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: Logger: MetricsRegistry: ExecutionTimeRecorder](
      subscriptionMechanism: SubscriptionMechanism[F]
  ): F[consumers.EventHandler[F]] = for {
    globalCommitEventSynchronizer <- CommitsSynchronizer[F]
    processExecutor               <- ProcessExecutor.concurrent(1)
  } yield new EventHandler[F](categoryName, globalCommitEventSynchronizer, subscriptionMechanism, processExecutor)
}
