/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.events.EventRequestContent
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.events.producers.EventSender
import ch.datascience.graph.model.events.EventStatus.{TransformationNonRecoverableFailure, TriplesGenerated}
import ch.datascience.tinytypes.json.TinyTypeEncoders
import io.circe.literal._
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.eventlog.{EventMessage, subscriptions}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private class DispatchRecoveryImpl[Interpretation[_]: MonadThrow](
    eventSender:   EventSender[Interpretation]
)(implicit logger: Logger[Interpretation])
    extends subscriptions.DispatchRecovery[Interpretation, TriplesGeneratedEvent]
    with TinyTypeEncoders {

  override def returnToQueue(event: TriplesGeneratedEvent): Interpretation[Unit] = eventSender.sendEvent(
    EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id},
        "project": {
          "id":   ${event.id.projectId},
          "path": ${event.projectPath}
        },
        "newStatus": $TriplesGenerated
      }"""),
    errorMessage = s"${SubscriptionCategory.name}: Marking event as $TriplesGenerated failed"
  )

  override def recover(
      url:   SubscriberUrl,
      event: TriplesGeneratedEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
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
      errorMessage = s"${SubscriptionCategory.name}: $event, url = $url -> $TransformationNonRecoverableFailure"
    ) >> logger.error(exception)(
      s"${SubscriptionCategory.name}: $event, url = $url -> $TransformationNonRecoverableFailure"
    )
  }
}

private object DispatchRecovery {

  def apply()(implicit
      effect:           ConcurrentEffect[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext,
      logger:           Logger[IO]
  ): IO[DispatchRecovery[IO, TriplesGeneratedEvent]] =
    EventSender() map (new DispatchRecoveryImpl[IO](_))
}
