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
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.http.ErrorMessage
import ch.datascience.metrics.LabeledHistogram
import io.circe.{Encoder, Json}
import io.renku.eventlog.{EventLogDB, EventMessage}
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventsEndpoint[Interpretation[_]] {
  def findEvents(projectPath: projects.Path): Interpretation[Response[Interpretation]]
}

class EventsEndpointImpl[Interpretation[_]: Effect: MonadThrow](eventsFinder: EventsFinder[Interpretation],
                                                                logger: Logger[Interpretation]
) extends Http4sDsl[Interpretation]
    with EventsEndpoint[Interpretation] {

  import cats.syntax.all._
  import ch.datascience.http.ErrorMessage._
  import io.circe.syntax._
  import org.http4s.circe._

  override def findEvents(projectPath: projects.Path): Interpretation[Response[Interpretation]] =
    eventsFinder
      .findEvents(projectPath)
      .flatMap(events => Ok(events.asJson))
      .recoverWith(httpResponse(projectPath))

  private def httpResponse(
      projectPath: projects.Path
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = { case NonFatal(exception) =>
    logger.error(exception)(s"Finding events for project '$projectPath' failed")
    InternalServerError(ErrorMessage(exception))
  }
}

object EventsEndpoint {

  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name],
            logger:            Logger[IO]
  )(implicit concurrentEffect: ConcurrentEffect[IO]): IO[EventsEndpoint[IO]] = for {
    eventsFinder <- EventsFinder(sessionResource, queriesExecTimes)
  } yield new EventsEndpointImpl(eventsFinder, logger)

  final case class EventInfo(eventId: EventId, status: EventStatus, maybeMessage: Option[EventMessage])

  object EventInfo {
    implicit lazy val infoEncoder: Encoder[EventInfo] = eventInfo => {
      import io.circe.literal._

      json"""{
        "id":     ${eventInfo.eventId.value},      
        "status": ${eventInfo.status.value}      
      }""" deepMerge {
        eventInfo.maybeMessage.map(message => json"""{"message": ${message.value}}""").getOrElse(Json.obj())
      }
    }
  }
}
