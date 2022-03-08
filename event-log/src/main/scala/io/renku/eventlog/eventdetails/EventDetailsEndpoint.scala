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

package io.renku.eventlog.eventdetails

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import io.renku.graph.model.events.{CompoundEventId, EventDetails}
import io.renku.http.InfoMessage._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.metrics.LabeledHistogram
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventDetailsEndpoint[F[_]] {
  def getDetails(eventId: CompoundEventId): F[Response[F]]
}

class EventDetailsEndpointImpl[F[_]: Concurrent: Logger](eventDetailsFinder: EventDetailsFinder[F])
    extends Http4sDsl[F]
    with EventDetailsEndpoint[F] {

  import org.http4s.circe._

  override def getDetails(eventId: CompoundEventId): F[Response[F]] =
    eventDetailsFinder.findDetails(eventId) flatMap {
      case Some(eventDetails) => Ok(eventDetails.asJson)
      case None               => NotFound(InfoMessage("Event not found"))
    } recoverWith internalServerError

  private lazy val internalServerError: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Finding event details failed")
    Logger[F].error(exception)(errorMessage.value)
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
  def apply[F[_]: Concurrent: Logger](sessionResource: SessionResource[F, EventLogDB],
                                      queriesExecTimes: LabeledHistogram[F]
  ): F[EventDetailsEndpoint[F]] = for {
    eventDetailFinder <- EventDetailsFinder(sessionResource, queriesExecTimes)
  } yield new EventDetailsEndpointImpl[F](eventDetailFinder)
}
