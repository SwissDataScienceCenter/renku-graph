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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.MonadThrow
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.circe.literal._
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.eventlog.{EventMessage, subscriptions}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.events.producers.EventSender
import io.renku.graph.model.events.EventStatus.{GenerationNonRecoverableFailure, New}
import io.renku.tinytypes.json.TinyTypeEncoders
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class DispatchRecoveryImpl[Interpretation[_]: MonadThrow: Logger](
    eventSender: EventSender[Interpretation]
) extends subscriptions.DispatchRecovery[Interpretation, AwaitingGenerationEvent]
    with TinyTypeEncoders {

  override def returnToQueue(event: AwaitingGenerationEvent): Interpretation[Unit] =
    eventSender.sendEvent(
      EventRequestContent.NoPayload(
        json"""{
          "categoryName": "EVENTS_STATUS_CHANGE",
          "id":           ${event.id.id},
          "project": {
            "id":   ${event.id.projectId},
            "path": ${event.projectPath}
          },
          "newStatus": $New
        }"""
      ),
      errorMessage = s"${SubscriptionCategory.name}: Marking event as $New failed"
    )

  override def recover(
      url:   SubscriberUrl,
      event: AwaitingGenerationEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    val requestContent = EventRequestContent.NoPayload(
      json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id},
        "project": {
          "id":   ${event.id.projectId},
          "path": ${event.projectPath}
        },
        "message" : ${EventMessage(exception)},
        "newStatus": $GenerationNonRecoverableFailure
      }"""
    )
    val errorMessage = s"${SubscriptionCategory.name}: $event, url = $url -> $GenerationNonRecoverableFailure"
    eventSender.sendEvent(requestContent, errorMessage) >> Logger[Interpretation].error(exception)(errorMessage)
  }
}

private object DispatchRecovery {
  def apply[F[_]: Async: Temporal: Logger]: F[DispatchRecovery[F, AwaitingGenerationEvent]] = for {
    eventSender <- EventSender[F]
  } yield new DispatchRecoveryImpl[F](eventSender)
}
