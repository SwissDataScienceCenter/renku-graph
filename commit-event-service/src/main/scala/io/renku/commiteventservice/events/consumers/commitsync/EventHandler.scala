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

package io.renku.commiteventservice.events.consumers.commitsync

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.commiteventservice.events.consumers.commitsync.eventgeneration.CommitsSynchronizer
import io.renku.events.{consumers, CategoryName}
import io.renku.events.consumers._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.graph.model.events.{CommitId, LastSyncedDate}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    commitsSynchronizer:       CommitsSynchronizer[F],
    subscriptionMechanism:     SubscriptionMechanism[F],
    processExecutor:           ProcessExecutor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = CommitSyncEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[CommitSyncEvent],
      process = commitsSynchronizer.synchronizeEvents,
      onRelease = subscriptionMechanism.renewSubscription()
    )

  import io.renku.tinytypes.json.TinyTypeDecoders._
  import EventDecodingTools._

  private implicit val eventDecoder: Decoder[CommitSyncEvent] = cursor =>
    cursor.downField("id").as[Option[CommitId]] >>= {
      case Some(id) =>
        for {
          project    <- cursor.value.getProject
          lastSynced <- cursor.downField("lastSynced").as[LastSyncedDate]
        } yield FullCommitSyncEvent(id, project, lastSynced)
      case None =>
        cursor.value.getProject.map(MinimalCommitSyncEvent)
    }
}

private object EventHandler {
  def apply[F[_]: Async: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: ExecutionTimeRecorder](
      subscriptionMechanism: SubscriptionMechanism[F]
  ): F[consumers.EventHandler[F]] = for {
    commitEventSynchronizer <- CommitsSynchronizer[F]
    processExecutor         <- ProcessExecutor.concurrent(10)
  } yield new EventHandler[F](categoryName, commitEventSynchronizer, subscriptionMechanism, processExecutor)
}
