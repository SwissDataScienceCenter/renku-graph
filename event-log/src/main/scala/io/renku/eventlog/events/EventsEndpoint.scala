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

package io.renku.eventlog.events

import cats.MonadThrow
import cats.effect.{ConcurrentEffect, Effect, IO}
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.http.ErrorMessage
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import ch.datascience.metrics.LabeledHistogram
import io.circe.{Encoder, Json}
import io.renku.eventlog._
import io.renku.eventlog.events.EventsEndpoint.EventInfo
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventsEndpoint[Interpretation[_]] {
  def findEvents(projectPath: projects.Path, pagingRequest: PagingRequest): Interpretation[Response[Interpretation]]
}

class EventsEndpointImpl[Interpretation[_]: Effect: MonadThrow: Logger](eventsFinder: EventsFinder[Interpretation],
                                                                        eventLogUrl: EventLogUrl
) extends Http4sDsl[Interpretation]
    with EventsEndpoint[Interpretation] {

  import ch.datascience.http.ErrorMessage._

  override def findEvents(projectPath:   projects.Path,
                          pagingRequest: PagingRequest
  ): Interpretation[Response[Interpretation]] =
    eventsFinder
      .findEvents(projectPath, pagingRequest)
      .map(_.toHttpResponse(Effect[Interpretation], requestedUrl(pagingRequest), EventLogUrl, EventInfo.infoEncoder))
      .recoverWith(httpResponse(projectPath))

  private def httpResponse(
      projectPath: projects.Path
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = { case NonFatal(exception) =>
    Logger[Interpretation].error(exception)(s"Finding events for project '$projectPath' failed")
    InternalServerError(ErrorMessage(exception))
  }
  private def requestedUrl(paging: PagingRequest): EventLogUrl =
    (eventLogUrl / "events") ? (page.parameterName -> paging.page) & (perPage.parameterName -> paging.perPage)

}

object EventsEndpoint {

  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name]
  )(implicit concurrentEffect: ConcurrentEffect[IO], logger: Logger[IO]): IO[EventsEndpoint[IO]] = for {
    eventsFinder <- EventsFinder(sessionResource, queriesExecTimes)
    eventlogUrl  <- EventLogUrl()
  } yield new EventsEndpointImpl(eventsFinder, eventlogUrl)

  final case class EventInfo(eventId:         EventId,
                             status:          EventStatus,
                             eventDate:       EventDate,
                             executionDate:   ExecutionDate,
                             maybeMessage:    Option[EventMessage],
                             processingTimes: List[StatusProcessingTime]
  )
  final case class StatusProcessingTime(status: EventStatus, processingTime: EventProcessingTime)

  object EventInfo {

    implicit lazy val infoEncoder: Encoder[EventInfo] = eventInfo => {
      import io.circe.literal._
      import io.circe.syntax._

      implicit val processingTimeEncoder: Encoder[StatusProcessingTime] = processingTime => json"""{
        "status":         ${processingTime.status.value},
        "processingTime": ${processingTime.processingTime.value}
      }"""

      json"""{
        "id":              ${eventInfo.eventId.value},
        "status":          ${eventInfo.status.value},
        "processingTimes": ${eventInfo.processingTimes.map(_.asJson)},
        "date" :           ${eventInfo.eventDate.value.asJson},
        "executionDate":   ${eventInfo.executionDate.value.asJson}
      }""" deepMerge {
        eventInfo.maybeMessage.map(message => json"""{"message": ${message.value}}""").getOrElse(Json.obj())
      }
    }
  }
}
