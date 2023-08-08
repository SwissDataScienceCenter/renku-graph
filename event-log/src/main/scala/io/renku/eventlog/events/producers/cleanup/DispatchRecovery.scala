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

package io.renku.eventlog.events.producers
package cleanup

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.circe.literal._
import io.renku.eventlog.events.producers.EventsSender.SendingResult
import io.renku.events.Subscription.SubscriberUrl
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.EventStatus._
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class DispatchRecoveryImpl[F[_]: MonadThrow: Logger](eventSender: EventSender[F])
    extends DispatchRecovery[F, CleanUpEvent] {

  override def returnToQueue(event: CleanUpEvent, reason: SendingResult): F[Unit] =
    sendStatusChangeEvent(event)

  override def recover(url: SubscriberUrl, event: CleanUpEvent): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      sendStatusChangeEvent(event) >>
        Logger[F].error(exception)(show"$categoryName: $event, url = $url -> $AwaitingDeletion")
  }

  private def sendStatusChangeEvent(event: CleanUpEvent) =
    eventSender.sendEvent(
      event.toAwaitingDeletionEvent,
      EventSender.EventContext(
        CategoryName("EVENTS_STATUS_CHANGE"),
        show"$categoryName: Marking events for project: ${event.project.slug} as $AwaitingDeletion failed"
      )
    )

  private implicit class CleanUpEventOps(event: CleanUpEvent) {

    def toAwaitingDeletionEvent = EventRequestContent.NoPayload(
      json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "project": {
          "id":   ${event.project.id},
          "slug": ${event.project.slug}
        },
        "subCategory": "RollbackToAwaitingDeletion"
     }"""
    )
  }
}

private object DispatchRecovery {
  def apply[F[_]: Async: Logger: MetricsRegistry]: F[DispatchRecovery[F, CleanUpEvent]] =
    EventSender[F](EventLogUrl).map(new DispatchRecoveryImpl[F](_))
}
