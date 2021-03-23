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

import cats.data.EitherT.fromEither
import cats.data.{EitherT, Kleisli, NonEmptyList}
import cats.effect.{Bracket, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, events, projects}
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.circe._
import io.renku.eventlog.statuschange.ChangeStatusRequest.{EventAndPayloadRequest, EventOnlyRequest}
import io.renku.eventlog.statuschange.CommandFindingResult._
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.{ChangeStatusRequest, CommandFindingResult}
import io.renku.eventlog.{EventLogDB, EventPayload, TypeSerializers}

import java.time.Instant

trait ToTriplesGenerated[Interpretation[_]] extends ChangeStatusCommand[Interpretation]

final case class GeneratingToTriplesGenerated[Interpretation[_]](
    eventId:                     CompoundEventId,
    payload:                     EventPayload,
    schemaVersion:               SchemaVersion,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
)(implicit ME:                   Bracket[Interpretation, Throwable])
    extends ToTriplesGenerated[Interpretation] {

  override lazy val status: events.EventStatus = TriplesGenerated

  override def queries: NonEmptyList[SqlQuery[Int]] = NonEmptyList(
    SqlQuery(
      query = updateStatus,
      name = "generating_triples->triples_generated"
    ),
    List(
      SqlQuery(
        query = upsertEventPayload,
        name = "upsert_generated_triples"
      )
    )
  )

  private lazy val updateStatus = sql"""|UPDATE event
                                        |SET status = $status, execution_date = ${now()}
                                        |WHERE event_id = ${eventId.id} 
                                        |  AND project_id = ${eventId.projectId} 
                                        |  AND status = ${GeneratingTriples: EventStatus}
                                        |""".stripMargin.update.run

  private lazy val upsertEventPayload = sql"""|INSERT INTO
                                              |event_payload (event_id, project_id, payload, schema_version)
                                              |VALUES (${eventId.id},  ${eventId.projectId}, $payload, $schemaVersion)
                                              |ON CONFLICT (event_id, project_id, schema_version)
                                              |DO UPDATE SET payload = EXCLUDED.payload;
                                              |""".stripMargin.update.run

  override def updateGauges(updateResult: UpdateResult)(implicit
      transactor:                         DbTransactor[Interpretation, EventLogDB]
  ): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- awaitingTransformationGauge increment path
        _    <- underTriplesGenerationGauge decrement path
      } yield ()
    case _ => ME.unit
  }
}

final case class TransformingToTriplesGenerated[Interpretation[_]](
    eventId:                         CompoundEventId,
    underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge:     LabeledGauge[Interpretation, projects.Path],
    now:                             () => Instant = () => Instant.now
)(implicit ME:                       Bracket[Interpretation, Throwable])
    extends ToTriplesGenerated[Interpretation] {

  override lazy val status: events.EventStatus = TriplesGenerated

  override def queries: NonEmptyList[SqlQuery[Int]] = NonEmptyList.of(
    SqlQuery(
      query = updateStatus,
      name = "transforming_triples->triples_generated"
    )
  )

  private lazy val updateStatus = sql"""|UPDATE event
                                        |SET status = $status, execution_date = ${now()}
                                        |WHERE event_id = ${eventId.id} 
                                        |  AND project_id = ${eventId.projectId} 
                                        |  AND status = ${TransformingTriples: EventStatus}
                                        |""".stripMargin.update.run

  override def updateGauges(updateResult: UpdateResult)(implicit
      transactor:                         DbTransactor[Interpretation, EventLogDB]
  ): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- awaitingTransformationGauge increment path
        _    <- underTriplesTransformationGauge decrement path
      } yield ()
    case _ => ME.unit
  }

  override val maybeProcessingTime: Option[EventProcessingTime] = None
}

private[statuschange] object ToTriplesGenerated extends TypeSerializers {

  def factory[Interpretation[_]: Sync: Bracket[*[_], Throwable]](
      transactor:                      DbTransactor[Interpretation, EventLogDB],
      underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
      underTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
      awaitingTransformationGauge:     LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli {
    case EventAndPayloadRequest(eventId, TriplesGenerated, maybeProcessingTime, payload) =>
      findEventStatus[Interpretation](eventId, transactor).flatMap {
        case GeneratingTriples =>
          {
            for {
              payloadJson <- stringToJson[Interpretation](payload)
              parsedPayload <-
                fromEither[Interpretation](payloadJson.as[(SchemaVersion, EventPayload)].toPayloadMalFormed)
            } yield CommandFound(
              GeneratingToTriplesGenerated[Interpretation](eventId,
                                                           parsedPayload._2,
                                                           parsedPayload._1,
                                                           underTriplesGenerationGauge,
                                                           awaitingTransformationGauge,
                                                           maybeProcessingTime
              )
            )
          }.merge.widen[CommandFindingResult]
        case _ => NotSupported.pure[Interpretation].widen[CommandFindingResult]
      }
    case EventOnlyRequest(eventId, TriplesGenerated, _, _) =>
      findEventStatus[Interpretation](eventId, transactor).map {
        case TransformingTriples =>
          CommandFound(
            TransformingToTriplesGenerated[Interpretation](eventId,
                                                           underTriplesTransformationGauge,
                                                           awaitingTransformationGauge
            )
          )
        case _ => NotSupported
      }
    case _ => NotSupported.pure[Interpretation].widen[CommandFindingResult]
  }

  private implicit class EitherOps[R](either: Either[Throwable, R]) {
    lazy val toPayloadMalFormed: Either[CommandFindingResult, R] =
      either.leftMap(e => PayloadMalformed(e.getMessage)).leftWiden[CommandFindingResult]
  }

  private def stringToJson[Interpretation[_]: Sync](
      payloadPartBody: String
  ): EitherT[Interpretation, CommandFindingResult, Json] =
    EitherT.fromEither[Interpretation](parser.parse(payloadPartBody).leftMap(e => PayloadMalformed(e.getMessage)))

  private implicit val payloadDecoder: Decoder[(SchemaVersion, EventPayload)] = (cursor: HCursor) =>
    for {
      schemaVersion <- cursor.downField("schemaVersion").as[SchemaVersion]
      payload       <- cursor.downField("payload").as[EventPayload]
    } yield (schemaVersion, payload)

  private def findEventStatus[Interpretation[_]: Sync: Bracket[*[_], Throwable]](
      eventId:    CompoundEventId,
      transactor: DbTransactor[Interpretation, EventLogDB]
  ): Interpretation[EventStatus] =
    sql"""|SELECT status
          |FROM event
          |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}
          |""".stripMargin
      .query[EventStatus]
      .unique
      .transact(transactor.get)

}
