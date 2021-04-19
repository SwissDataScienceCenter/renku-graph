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
import cats.effect.{Async, Bracket, Sync}
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, events, projects}
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.auto._
import io.circe._
import io.renku.eventlog.statuschange.ChangeStatusRequest.{EventAndPayloadRequest, EventOnlyRequest}
import io.renku.eventlog.statuschange.CommandFindingResult._
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.{ChangeStatusRequest, CommandFindingResult}
import io.renku.eventlog.{EventLogDB, EventPayload, ExecutionDate, TypeSerializers}
import skunk.data.Completion
import skunk.implicits._
import skunk.{Command, Session, ~, _}

import java.time.Instant

trait ToTriplesGenerated[Interpretation[_]] extends ChangeStatusCommand[Interpretation]

final case class GeneratingToTriplesGenerated[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    eventId:                     CompoundEventId,
    payload:                     EventPayload,
    schemaVersion:               SchemaVersion,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
) extends ToTriplesGenerated[Interpretation] {

  override lazy val status: events.EventStatus = TriplesGenerated

  override def queries: NonEmptyList[SqlQuery[Interpretation, Int]] = NonEmptyList(
    SqlQuery[Interpretation, Int](
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

  private lazy val updateStatus = Kleisli[Interpretation, Session[Interpretation], Int] { session =>
    val query: Command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus] = sql"""
      UPDATE event
      SET status = $eventStatusPut, execution_date = $executionDatePut
      WHERE event_id = $eventIdPut
        AND project_id = $projectIdPut
        AND status = $eventStatusPut;
      """.command
    session
      .prepare(query)
      .use(_.execute(status ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ GeneratingTriples))
      .map {
        case Completion.Update(n) => n
        case completion =>
          throw new RuntimeException(
            s"generating_triples->triples_generated time query failed with completion status $completion"
          )
      }
  }

  private lazy val upsertEventPayload = Kleisli[Interpretation, Session[Interpretation], Int] { session =>
    val query: Command[EventId ~ projects.Id ~ EventPayload ~ SchemaVersion] = sql"""
            INSERT INTO event_payload (event_id, project_id, payload, schema_version)
            VALUES ($eventIdPut,  $projectIdPut, $eventPayloadPut, $schemaVersionPut)
            ON CONFLICT (event_id, project_id, schema_version)
            DO UPDATE SET payload = EXCLUDED.payload;
          """.command
    session.prepare(query).use(_.execute(eventId.id ~ eventId.projectId ~ payload ~ schemaVersion)).map {
      case Completion.Insert(n) => n
      case completion =>
        throw new RuntimeException(s"upsert_generated_triples time query failed with completion status $completion")
    }
  }
  override def updateGauges(updateResult: UpdateResult)(implicit
      session:                            Session[Interpretation]
  ): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- awaitingTransformationGauge increment path
        _    <- underTriplesGenerationGauge decrement path
      } yield ()
    case _ => ().pure[Interpretation]
  }
}

final case class TransformingToTriplesGenerated[Interpretation[_]: Bracket[*[_], Throwable]](
    eventId:                         CompoundEventId,
    awaitingTransformationGauge:     LabeledGauge[Interpretation, projects.Path],
    underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    now:                             () => Instant = () => Instant.now
) extends ToTriplesGenerated[Interpretation] {

  override lazy val status: events.EventStatus = TriplesGenerated

  override def queries: NonEmptyList[SqlQuery[Interpretation, Int]] = NonEmptyList.of(
    SqlQuery(
      query = updateStatus,
      name = "transforming_triples->triples_generated"
    )
  )

  private lazy val updateStatus = Kleisli[Interpretation, Session[Interpretation], Int] { session =>
    val query: Command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus] = sql"""
      UPDATE event
      SET status = $eventStatusPut, execution_date = $executionDatePut
      WHERE event_id = $eventIdPut
        AND project_id = $projectIdPut
        AND status = $eventStatusPut;
      """.command
    session
      .prepare(query)
      .use(_.execute(status ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ TransformingTriples))
      .map {
        case Completion.Update(n) => n
        case completion =>
          throw new RuntimeException(
            s"generating_triples->triples_generated time query failed with completion status $completion"
          )
      }
  }

  override def updateGauges(updateResult: UpdateResult)(implicit
      session:                            Session[Interpretation]
  ): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- awaitingTransformationGauge increment path
        _    <- underTriplesTransformationGauge decrement path
      } yield ()
    case _ => ().pure[Interpretation]
  }

  override val maybeProcessingTime: Option[EventProcessingTime] = None
}

private[statuschange] object ToTriplesGenerated extends TypeSerializers {

  def factory[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor:                      SessionResource[Interpretation, EventLogDB],
      underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
      underTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
      awaitingTransformationGauge:     LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli {
    case EventAndPayloadRequest(_, TriplesGenerated, None, _) =>
      (PayloadMalformed("No processing time provided"): CommandFindingResult).pure[Interpretation]
    case EventAndPayloadRequest(eventId, TriplesGenerated, Some(processingTime), payload) =>
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
                                                           Some(processingTime)
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
                                                           awaitingTransformationGauge,
                                                           underTriplesTransformationGauge
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

  private implicit val payloadDecoder: io.circe.Decoder[(SchemaVersion, EventPayload)] = (cursor: HCursor) =>
    for {
      schemaVersion <- cursor.downField("schemaVersion").as[SchemaVersion]
      payload       <- cursor.downField("payload").as[EventPayload]
    } yield (schemaVersion, payload)

  private def findEventStatus[Interpretation[_]: Sync: Bracket[*[_], Throwable]](
      eventId:    CompoundEventId,
      transactor: SessionResource[Interpretation, EventLogDB]
  ): Interpretation[EventStatus] = transactor.use { session =>
    val query: Query[EventId ~ projects.Id, EventStatus] = sql"""
            SELECT status
            FROM event
            WHERE event_id = $eventIdPut AND project_id = $projectIdPut
            """.query(eventStatusGet)
    session.prepare(query).use(_.unique(eventId.id ~ eventId.projectId))
  }

}
