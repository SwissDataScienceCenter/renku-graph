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

package io.renku.eventlog.eventdetails

import cats.effect.{Effect, IO}
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.graph.model.events.{CompoundEventId, EventDetails}
import io.renku.http.InfoMessage._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.metrics.LabeledHistogram
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventDetailsEndpoint[Interpretation[_]] {
  def getDetails(eventId: CompoundEventId): Interpretation[Response[Interpretation]]
}

class EventDetailsEndpointImpl[Interpretation[_]: Effect](eventDetailsFinder: EventDetailsFinder[Interpretation],
                                                          logger: Logger[Interpretation]
) extends Http4sDsl[Interpretation]
    with EventDetailsEndpoint[Interpretation] {

  import org.http4s.circe._

  override def getDetails(eventId: CompoundEventId): Interpretation[Response[Interpretation]] =
    eventDetailsFinder.findDetails(eventId).flatMap {
      case Some(eventDetails) => Ok(eventDetails.asJson)
      case None               => NotFound(InfoMessage("Event not found"))
    } recoverWith internalServerError

  private lazy val internalServerError: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Finding event details failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private implicit lazy val encoder: Encoder[EventDetails] = Encoder.instance[EventDetails] { eventDetails =>
    json"""{
      "id": ${eventDetails.id.value},
      "project": {
        "id": ${eventDetails.projectId.value}
      },
      "body": ${eventDetails.eventBody.value}
    }"""
  }
}

object EventDetailsEndpoint {
  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name],
            logger:           Logger[IO]
  ): IO[EventDetailsEndpoint[IO]] = for {
    eventDetailFinder <- EventDetailsFinder(sessionResource, queriesExecTimes)
  } yield new EventDetailsEndpointImpl[IO](eventDetailFinder, logger)
}
