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

package io.renku.commiteventservice.events.categories.common

import cats.MonadThrow
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.common.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.EventBody
import io.renku.http.client.RestClient
import org.http4s.Status
import org.http4s.Status.Accepted
import org.typelevel.log4cats.Logger

private[categories] trait CommitEventSender[Interpretation[_]] {
  def send(commitEvent: CommitEvent): Interpretation[Unit]
}

private[categories] class CommitEventSenderImpl[Interpretation[_]: Async: Temporal: Logger](
    eventLogUrl:           EventLogUrl,
    commitEventSerializer: CommitEventSerializer[Interpretation]
) extends RestClient[Interpretation, CommitEventSender[Interpretation]](Throttler.noThrottling)
    with CommitEventSender[Interpretation] {

  import commitEventSerializer._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.{Request, Response}

  def send(commitEvent: CommitEvent): Interpretation[Unit] = for {
    serialisedEvent <- serialiseToJsonString(commitEvent)
    eventBody       <- MonadThrow[Interpretation].fromEither(EventBody.from(serialisedEvent))
    uri             <- validateUri(s"$eventLogUrl/events")
    sendingResult <- send(
                       request(POST, uri).withMultipartBuilder
                         .addPart("event", (commitEvent -> eventBody).asJson)
                         .build()
                     )(mapResponse)
  } yield sendingResult

  private implicit lazy val entityEncoder: Encoder[(CommitEvent, EventBody)] =
    Encoder.instance[(CommitEvent, EventBody)] {
      case (event: NewCommitEvent, body) =>
        json"""{
        "categoryName": "CREATION", 
        "id":        ${event.id.value},
        "project": {
          "id":      ${event.project.id.value},
          "path":    ${event.project.path.value}
        },
        "date":      ${event.committedDate.value},
        "batchDate": ${event.batchDate.value},
        "body":      ${body.value},
        "status":    ${event.status.value}
      }"""
      case (event: SkippedCommitEvent, body) =>
        json"""{
        "categoryName": "CREATION",
        "id":        ${event.id.value},
        "project": {
          "id":      ${event.project.id.value},
          "path":    ${event.project.path.value}
        },
        "date":      ${event.committedDate.value},
        "batchDate": ${event.batchDate.value},
        "body":      ${body.value},
        "status":    ${event.status.value},
        "message":   ${event.message.value}
      }"""
    }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted, _, _) => ().pure[Interpretation]
  }
}

private[categories] object CommitEventSender {
  def apply[Interpretation[_]: Async: Temporal: Logger]: Interpretation[CommitEventSender[Interpretation]] = for {
    eventLogUrl <- EventLogUrl[Interpretation]()
  } yield new CommitEventSenderImpl(eventLogUrl, new CommitEventSerializer[Interpretation])
}
