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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.circe.literal._
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.eventlog.{EventMessage, subscriptions}
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.events.producers.EventSender
import io.renku.graph.model.events.EventStatus.{TransformationNonRecoverableFailure, TriplesGenerated}
import io.renku.metrics.MetricsRegistry
import io.renku.tinytypes.json.TinyTypeEncoders
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class DispatchRecoveryImpl[F[_]: MonadThrow: Logger](
    eventSender: EventSender[F]
) extends subscriptions.DispatchRecovery[F, TriplesGeneratedEvent]
    with TinyTypeEncoders {

  override def returnToQueue(event: TriplesGeneratedEvent): F[Unit] = eventSender.sendEvent(
    EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id},
        "project": {
          "id":   ${event.id.projectId},
          "path": ${event.projectPath}
        },
        "newStatus": $TriplesGenerated
      }"""),
    EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                             errorMessage =
                               s"${SubscriptionCategory.categoryName}: Marking event as $TriplesGenerated failed"
    )
  )

  override def recover(
      url:   SubscriberUrl,
      event: TriplesGeneratedEvent
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    eventSender.sendEvent(
      EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id},
        "project": {
          "id":   ${event.id.projectId},
          "path": ${event.projectPath}
        },
        "newStatus": $TransformationNonRecoverableFailure,
        "message":   ${EventMessage(exception)} }"""),
      EventSender.EventContext(
        CategoryName("EVENTS_STATUS_CHANGE"),
        errorMessage =
          s"${SubscriptionCategory.categoryName}: $event, url = $url -> $TransformationNonRecoverableFailure"
      )
    ) >> Logger[F].error(exception)(
      s"${SubscriptionCategory.categoryName}: $event, url = $url -> $TransformationNonRecoverableFailure"
    )
  }
}

private object DispatchRecovery {

  def apply[F[_]: Async: Logger: MetricsRegistry]: F[DispatchRecovery[F, TriplesGeneratedEvent]] =
    EventSender[F].map(new DispatchRecoveryImpl[F](_))
}
