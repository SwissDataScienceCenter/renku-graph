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

package io.renku.eventlog.events.categories.statuschange

import cats.MonadThrow
import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift}
import cats.syntax.all._
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import io.circe.{Decoder, DecodingFailure}
import org.typelevel.log4cats.Logger
import StatusChangeEvent._

import scala.util.control.NonFatal

private class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    override val categoryName: CategoryName,
    statusChanger:             StatusChanger[Interpretation],
    logger:                    Logger[Interpretation]
) extends consumers.EventHandler[Interpretation] {

  import EventHandler._

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      _ <- fromEither[Interpretation](request.event.validateCategoryName)
      event <- fromEither[Interpretation](
                 request.event
                   .as[StatusChangeEvent.TriplesGenerated]
                   .orElse(request.event.as[StatusChangeEvent.TriplesStore])
                   .leftMap(_ => BadRequest)
                   .leftWiden[EventSchedulingResult]
               )

      result <- (ContextShift[Interpretation].shift *> Concurrent[Interpretation].start(
                  statusChanger.updateStatuses(event) >> logger.logInfo(event, "Processed") recoverWith {
                    case NonFatal(e) => logger.logError(event, e)
                  }
                )).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(event.show))

    } yield result
  }.merge

}

private object EventHandler {
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  private implicit val eventTriplesGeneratedDecoder: Decoder[StatusChangeEvent.TriplesGenerated] = { cursor =>
    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      _ <- cursor.downField("newStatus").as[EventStatus].flatMap {
             case EventStatus.TriplesGenerated => Right(())
             case status                       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield StatusChangeEvent.TriplesGenerated(CompoundEventId(id, projectId), projectPath)
  }
  private implicit val eventTripleStoreDecoder: Decoder[StatusChangeEvent.TriplesStore] = { cursor =>
    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      _ <- cursor.downField("newStatus").as[EventStatus].flatMap {
             case EventStatus.TriplesStore => Right(())
             case status                   => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield StatusChangeEvent.TriplesStore(CompoundEventId(id, projectId), projectPath)
  }
}
