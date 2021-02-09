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
import cats.data.EitherT.{fromEither, fromOption, right}
import cats.data.{EitherT, Kleisli, NonEmptyList}
import cats.effect.{Bracket, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, events, projects}
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.circe.{Decoder, HCursor, Json, parser}
import io.renku.eventlog.statuschange.commands.CommandFindingResult._
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.{EventLogDB, EventPayload, EventProcessingTime}
import org.http4s.multipart.{Multipart, Part}
import org.http4s.{MediaType, Request}

import java.time.Instant
import scala.util.control.NonFatal

private[statuschange] final case class ToTriplesGenerated[Interpretation[_]](
    eventId:                     CompoundEventId,
    payload:                     EventPayload,
    schemaVersion:               SchemaVersion,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
)(implicit ME:                   Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {
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
                                        |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${GeneratingTriples: EventStatus};
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

object ToTriplesGenerated {
  import org.http4s.circe.jsonDecoder
  def factory[Interpretation[_]: Sync](underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
                                       awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), CommandFindingResult] =
    Kleisli { case (eventId, request) =>
      when(request, has = MediaType.multipart.`form-data`) {
        {
          for {
            multipart <- right[CommandFindingResult](request.as[Multipart[Interpretation]])
            eventJson <- fromOption[Interpretation](multipart.parts.find(_.name.contains("event")),
                                                    PayloadMalformed("No event part in change status payload")
                         ).flatMapF(toJson[Interpretation]).leftWiden[CommandFindingResult]
            _                   <- fromEither[Interpretation](eventJson.validate(status = TriplesGenerated))
            maybeProcessingTime <- fromEither[Interpretation](eventJson.getProcessingTime)
            payloadJson <- fromOption[Interpretation](multipart.parts.find(_.name.contains("payload")),
                                                      PayloadMalformed("No payload part in change status payload")
                           ).flatMap(stringToJson[Interpretation])
            parsedPayload <-
              fromEither[Interpretation](payloadJson.as[(SchemaVersion, EventPayload)].toPayloadMalFormed)
          } yield CommandFound(
            ToTriplesGenerated[Interpretation](eventId,
                                               parsedPayload._2,
                                               parsedPayload._1,
                                               underTriplesGenerationGauge,
                                               awaitingTransformationGauge,
                                               maybeProcessingTime
            )
          )
        }.merge
      }
    }

  private implicit class EitherOps[R](either: Either[Throwable, R]) {
    lazy val toPayloadMalFormed: Either[CommandFindingResult, R] =
      either.leftMap(e => PayloadMalformed(e.getMessage)).leftWiden[CommandFindingResult]
  }

  private def toJson[Interpretation[_]: Sync](
      part: Part[Interpretation]
  ): Interpretation[Either[CommandFindingResult, Json]] =
    part.as[Json].map(_.asRight[CommandFindingResult]).recover { case NonFatal(_) =>
      PayloadMalformed("Cannot parse change status payload").asLeft[Json]
    }

  private def stringToJson[Interpretation[_]: Sync](
      part: Part[Interpretation]
  ): EitherT[Interpretation, CommandFindingResult, Json] =
    EitherT(part.as[String].map(_.asRight[CommandFindingResult]).recover { case NonFatal(_) =>
      PayloadMalformed("Cannot parse change status payload").asLeft[String]
    }).subflatMap[CommandFindingResult, Json](parser.parse(_).leftMap(e => PayloadMalformed(e.getMessage)))

  private implicit val payloadDecoder: Decoder[(SchemaVersion, EventPayload)] = (cursor: HCursor) =>
    for {
      schemaVersion <- cursor.downField("schemaVersion").as[SchemaVersion]
      payload       <- cursor.downField("payload").as[EventPayload]
    } yield (schemaVersion, payload)

}
