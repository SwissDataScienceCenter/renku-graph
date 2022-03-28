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

package io.renku.webhookservice

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.model.CommitSyncRequest
import org.typelevel.log4cats.Logger

private trait CommitSyncRequestSender[F[_]] {
  def sendCommitSyncRequest(commitSyncRequest: CommitSyncRequest, processName: String): F[Unit]
}

private class CommitSyncRequestSenderImpl[F[_]: MonadThrow: Logger](eventSender: EventSender[F])
    extends CommitSyncRequestSender[F] {

  import eventSender._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._

  def sendCommitSyncRequest(syncRequest: CommitSyncRequest, processName: String): F[Unit] =
    sendEvent(
      EventRequestContent.NoPayload(syncRequest.asJson),
      EventSender.EventContext(CategoryName("COMMIT_SYNC_REQUEST"),
                               show"$processName - sending COMMIT_SYNC_REQUEST for ${syncRequest.project} failed"
      )
    ) >> logInfo(processName)(syncRequest)

  private implicit lazy val entityEncoder: Encoder[CommitSyncRequest] = Encoder.instance[CommitSyncRequest] { event =>
    json"""{
      "categoryName": "COMMIT_SYNC_REQUEST",
      "project": {
        "id":   ${event.project.id.value},
        "path": ${event.project.path.value}
      }
    }"""
  }

  private def logInfo(processName: String): CommitSyncRequest => F[Unit] = { case CommitSyncRequest(project) =>
    Logger[F].info(show"$processName - COMMIT_SYNC_REQUEST sent for $project")
  }
}

private object CommitSyncRequestSender {
  def apply[F[_]: Async: Logger: MetricsRegistry]: F[CommitSyncRequestSender[F]] = for {
    eventSender <- EventSender[F]
  } yield new CommitSyncRequestSenderImpl(eventSender)
}
