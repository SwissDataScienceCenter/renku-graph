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

package io.renku.eventlog.statuschange.commands

import cats.MonadError
import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Bracket, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeOption
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.renku.eventlog.{EventLogDB, EventMessage, EventProcessingTime}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Request}

import java.time.Instant

final case class ToTriplesStore[Interpretation[_]](
    eventId:                     CompoundEventId,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
)(implicit ME:                   Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = TriplesStore

  override def queries: NonEmptyList[SqlQuery[Int]] = NonEmptyList(
    SqlQuery(
      query = upsertEventStatus,
      name = "transforming_triples->triples_store"
    ),
    Nil
  )

  lazy val upsertEventStatus =
    sql"""|UPDATE event
          |SET status = $status, execution_date = ${now()}
          |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${TransformingTriples: EventStatus}
          |""".stripMargin.update.run

  override def updateGauges(
      updateResult:      UpdateResult
  )(implicit transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated => findProjectPath(eventId) flatMap underTriplesGenerationGauge.decrement
    case _                    => ME.unit
  }
}

object ToTriplesStore {
  def factory[Interpretation[_]: Sync](
      underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), Option[
    ChangeStatusCommand[Interpretation]
  ]] =
    Kleisli { eventIdAndRequest =>
      val (eventId, request) = eventIdAndRequest
      (for {
        maybeProcessingTime <- request.as[Option[EventProcessingTime]](ME, entityDecoder[Interpretation]())
      } yield (ToTriplesStore[Interpretation](
        eventId,
        underTriplesGenerationGauge,
        maybeProcessingTime
      ): ChangeStatusCommand[Interpretation]).some) recoverWith (_ =>
        Option.empty[ChangeStatusCommand[Interpretation]].pure[Interpretation]
      )

    }

  private def entityDecoder[Interpretation[_]: Sync](): EntityDecoder[Interpretation, Option[EventProcessingTime]] = {
    implicit val decoder: Decoder[Option[EventProcessingTime]] = { (cursor: HCursor) =>
      (for {
        maybeStatus <- cursor.downField("status").as[EventStatus]
        maybeProcessingTime <-
          cursor.downField("processingTime").as[Option[EventProcessingTime]](decodeOption(EventProcessingTime.decoder))
      } yield maybeStatus match {
        case EventStatus.TriplesStore => Right(maybeProcessingTime)
        case _                        => Left(DecodingFailure("Invalid event status", Nil))
      }).flatten
    }
    jsonOf[Interpretation, Option[EventProcessingTime]]
  }
}
