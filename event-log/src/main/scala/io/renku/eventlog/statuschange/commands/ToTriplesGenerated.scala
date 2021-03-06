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

import cats.{Applicative, Id}
import cats.data.EitherT.fromEither
import cats.data.{EitherT, Kleisli, NonEmptyList}
import cats.effect.{Bracket, BracketThrow}
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlStatement}
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
import skunk.{Command, Session, ~}

import java.time.Instant

trait ToTriplesGenerated[Interpretation[_]] extends ChangeStatusCommand[Interpretation]

final case class GeneratingToTriplesGenerated[Interpretation[_]: BracketThrow](
    eventId:                     CompoundEventId,
    payload:                     EventPayload,
    schemaVersion:               SchemaVersion,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
) extends ToTriplesGenerated[Interpretation] {

  override lazy val status: events.EventStatus = TriplesGenerated

  override def queries: NonEmptyList[SqlStatement[Interpretation, Int]] = NonEmptyList(
    SqlStatement[Interpretation](name = "generating_triples->triples_generated")
      .command(updateStatus)
      .arguments(status ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ GeneratingTriples)
      .build
      .flatMapResult {
        case Completion.Update(n) => n.pure[Interpretation]
        case completion =>
          new RuntimeException(
            s"generating_triples->triples_generated time query failed with completion status $completion"
          ).raiseError[Interpretation, Int]
      },
    List(
      SqlStatement[Interpretation](name = "upsert_generated_triples")
        .command(upsertEventPayload)
        .arguments(eventId.id ~ eventId.projectId ~ payload ~ schemaVersion)
        .build
        .flatMapResult {
          case Completion.Insert(n) => n.pure[Interpretation]
          case completion =>
            new RuntimeException(s"upsert_generated_triples time query failed with completion status $completion")
              .raiseError[Interpretation, Int]
        }
    )
  )

  private lazy val updateStatus: Command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus] =
    sql"""UPDATE event
            SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
            WHERE event_id = $eventIdEncoder
              AND project_id = $projectIdEncoder
              AND status = $eventStatusEncoder;
      """.command

  private lazy val upsertEventPayload: Command[EventId ~ projects.Id ~ EventPayload ~ SchemaVersion] =
    sql"""
            INSERT INTO event_payload (event_id, project_id, payload, schema_version)
            VALUES ($eventIdEncoder,  $projectIdEncoder, $eventPayloadEncoder, $schemaVersionEncoder)
            ON CONFLICT (event_id, project_id, schema_version)
            DO UPDATE SET payload = EXCLUDED.payload;
          """.command

  override def updateGauges(updateResult: UpdateResult): Kleisli[Interpretation, Session[Interpretation], Unit] =
    updateResult match {
      case UpdateResult.Updated =>
        for {
          path <- findProjectPath(eventId)
          _    <- Kleisli.liftF(awaitingTransformationGauge increment path)
          _    <- Kleisli.liftF(underTriplesGenerationGauge decrement path)
        } yield ()
      case _ => Kleisli.pure(())
    }
}

final case class TransformingToTriplesGenerated[Interpretation[_]: BracketThrow](
    eventId:                         CompoundEventId,
    awaitingTransformationGauge:     LabeledGauge[Interpretation, projects.Path],
    underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    now:                             () => Instant = () => Instant.now
) extends ToTriplesGenerated[Interpretation] {

  override lazy val status: events.EventStatus = TriplesGenerated

  override def queries: NonEmptyList[SqlStatement[Interpretation, Int]] = NonEmptyList.of(
    SqlStatement(name = "transforming_triples->triples_generated")
      .command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus](
        sql"""UPDATE event
              SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder
                AND project_id = $projectIdEncoder
                AND status = $eventStatusEncoder;
      """.command
      )
      .arguments(status ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ TransformingTriples)
      .build
      .flatMapResult {
        case Completion.Update(n) => n.pure[Interpretation]
        case completion =>
          new RuntimeException(
            s"transforming_triples->triples_generated time query failed with completion status $completion"
          ).raiseError[Interpretation, Int]
      }
  )

  override def updateGauges(updateResult: UpdateResult): Kleisli[Interpretation, Session[Interpretation], Unit] =
    updateResult match {
      case UpdateResult.Updated =>
        for {
          path <- findProjectPath(eventId)
          _    <- Kleisli.liftF(awaitingTransformationGauge increment path)
          _    <- Kleisli.liftF(underTriplesTransformationGauge decrement path)
        } yield ()
      case _ => Kleisli.pure(())
    }

  override val maybeProcessingTime: Option[EventProcessingTime] = None
}

private[statuschange] object ToTriplesGenerated extends TypeSerializers {

  def factory[Interpretation[_]: Bracket[*[_], Throwable]](
      sessionResource:                 SessionResource[Interpretation, EventLogDB],
      underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
      underTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
      awaitingTransformationGauge:     LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli {
    case EventAndPayloadRequest(_, TriplesGenerated, None, _) =>
      (PayloadMalformed("No processing time provided"): CommandFindingResult).pure[Interpretation]
    case EventAndPayloadRequest(eventId, TriplesGenerated, Some(processingTime), payload) =>
      findEventStatus[Interpretation](eventId, sessionResource).flatMap {
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
      findEventStatus[Interpretation](eventId, sessionResource).map {
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

  private def stringToJson[Interpretation[_]: Applicative](
      payloadPartBody: String
  ): EitherT[Interpretation, CommandFindingResult, Json] =
    EitherT.fromEither[Interpretation](parser.parse(payloadPartBody).leftMap(e => PayloadMalformed(e.getMessage)))

  private implicit val payloadDecoder: io.circe.Decoder[(SchemaVersion, EventPayload)] = (cursor: HCursor) =>
    for {
      schemaVersion <- cursor.downField("schemaVersion").as[SchemaVersion]
      payload       <- cursor.downField("payload").as[EventPayload]
    } yield (schemaVersion, payload)

  private def findEventStatus[Interpretation[_]: BracketThrow](
      eventId:         CompoundEventId,
      sessionResource: SessionResource[Interpretation, EventLogDB]
  ): Interpretation[EventStatus] = sessionResource.useK {
    SqlStatement(name = "to_triples_generated find event status")
      .select[EventId ~ projects.Id, EventStatus](
        sql"""
            SELECT status
            FROM event
            WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.query(eventStatusDecoder)
      )
      .arguments(eventId.id ~ eventId.projectId)
      .build[Id](_.unique)
      .queryExecution
  }

}
