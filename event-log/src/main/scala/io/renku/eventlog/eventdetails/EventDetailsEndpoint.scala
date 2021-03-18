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
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.http.InfoMessage._
import ch.datascience.metrics.LabeledHistogram
import io.chrisdavenport.log4cats.Logger
import io.circe.Encoder
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps
import io.renku.eventlog.EventLogDB
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

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
      case Some(eventId) => Ok(eventId.asJson)
      case None          => NotFound(InfoMessage("Event not found"))
    } recoverWith internalServerError

  private lazy val internalServerError: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Finding event details failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private implicit lazy val encoder: Encoder[CompoundEventId] = Encoder.instance[CompoundEventId] { eventId =>
    json"""{
      "id":     ${eventId.id.value},
      "project": {
        "id":   ${eventId.projectId.value}
      }
    }"""
  }
}

object EventDetailsEndpoint {
  def apply(transactor:       SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  ): IO[EventDetailsEndpoint[IO]] = for {
    eventDetailFinder <- EventDetailsFinder(transactor, queriesExecTimes)
  } yield new EventDetailsEndpointImpl[IO](eventDetailFinder, logger)
}
