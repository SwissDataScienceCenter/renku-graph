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

package io.renku.eventlog.subscriptions.cleanup

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.circe.literal._
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.eventlog.subscriptions
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.events.producers.EventSender
import io.renku.tinytypes.json.TinyTypeEncoders
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal
import io.renku.graph.model.events.EventStatus._
import io.renku.metrics.MetricsRegistry

private class DispatchRecoveryImpl[F[_]: MonadThrow: Logger](
    eventSender: EventSender[F]
) extends subscriptions.DispatchRecovery[F, CleanUpEvent]
    with TinyTypeEncoders {

  override def returnToQueue(event: CleanUpEvent): F[Unit] =
    eventSender.sendEvent(
      sendEventPayload(event),
      EventSender.EventContext(
        CategoryName("EVENTS_STATUS_CHANGE"),
        s"${SubscriptionCategory.name}: Marking events for project: ${event.project.path} as $AwaitingDeletion failed"
      )
    )

  override def recover(
      url:   SubscriberUrl,
      event: CleanUpEvent
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    val errorMessage = s"${SubscriptionCategory.name}: $event, url = $url -> $AwaitingDeletion"
    eventSender.sendEvent(sendEventPayload(event),
                          EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"), errorMessage)
    ) >> Logger[F].error(exception)(errorMessage)
  }

  private def sendEventPayload(event: CleanUpEvent) =
    EventRequestContent.NoPayload(
      json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "project": {
          "id":   ${event.project.id},
          "path": ${event.project.path}
        },
        "newStatus": $AwaitingDeletion
     }"""
    )

}

private object DispatchRecovery {
  def apply[F[_]: Async: Logger: MetricsRegistry]: F[DispatchRecovery[F, CleanUpEvent]] = for {
    eventSender <- EventSender[F]
  } yield new DispatchRecoveryImpl[F](eventSender)
}
