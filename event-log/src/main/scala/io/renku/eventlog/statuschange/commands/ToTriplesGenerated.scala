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
import cats.data.{EitherT, Kleisli, NonEmptyList, OptionT}
import cats.effect.{Bracket, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, events, projects}
import ch.datascience.metrics.LabeledGauge
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeOption
import io.circe.{Decoder, DecodingFailure, HCursor, Json, parser}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.{EventLogDB, EventMessage, EventPayload, EventProcessingTime}
import org.http4s.circe.jsonOf
import org.http4s.multipart.Multipart
import org.http4s.{EntityDecoder, Request}

import java.time.Instant

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
  ): Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), Option[
    ChangeStatusCommand[Interpretation]
  ]] =
    Kleisli { eventIdAndRequest =>
      val (eventId, request) = eventIdAndRequest
      (for {
        multipart <- OptionT.liftF(request.as[Multipart[Interpretation]])
        eventJson <-
          OptionT.liftF(multipart.parts.find(_.name.contains("event")).map(_.as[Json]).get)
        payloadStr <-
          OptionT.liftF(
            multipart.parts.find(_.name.contains("payload")).map(_.as[String]).get
          )

        maybeProcessingTime <-
          OptionT.fromOption[Interpretation](eventJson.as[Option[EventProcessingTime]](decoder).toOption)
        payloadJson <- OptionT.fromOption[Interpretation](parser.parse(payloadStr).toOption)
        parsedPayload <- OptionT
                           .fromOption[Interpretation](payloadJson.as[(SchemaVersion, EventPayload)].toOption)
        (schemaVersion, eventPayload) = parsedPayload
      } yield (ToTriplesGenerated[Interpretation](eventId,
                                                  eventPayload,
                                                  schemaVersion,
                                                  underTriplesGenerationGauge,
                                                  awaitingTransformationGauge,
                                                  maybeProcessingTime
      ): ChangeStatusCommand[Interpretation])).value recoverWith (_ =>
        Option.empty[ChangeStatusCommand[Interpretation]].pure[Interpretation]
      )

    }

  implicit val decoder: Decoder[Option[EventProcessingTime]] = { (cursor: HCursor) =>
    (for {
      status <- cursor.downField("status").as[EventStatus]
      maybeProcessingTime <-
        cursor.downField("processingTime").as[Option[EventProcessingTime]](decodeOption(EventProcessingTime.decoder))
    } yield status match {
      case EventStatus.TriplesGenerated => Right(maybeProcessingTime)
      case _                            => Left(DecodingFailure("Invalid event status", Nil))
    }).flatten
  }

  implicit val payloadDecoder: Decoder[(SchemaVersion, EventPayload)] = (cursor: HCursor) =>
    for {
      schemaVersion <- cursor.downField("schemaVersion").as[SchemaVersion]
      payload       <- cursor.downField("payload").as[EventPayload]
    } yield (schemaVersion, payload)

}
