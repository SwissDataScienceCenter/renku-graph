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

package io.renku.eventlog.statuschange

import cats.MonadError
import cats.effect.{ContextShift, Effect}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import io.renku.eventlog.{EventLogDB, EventMessage, EventPayload, EventProcessingTime, ExecutionDate}
import org.http4s.dsl.Http4sDsl

import java.time.Instant
import scala.util.control.NonFatal

class StatusChangeEndpoint[Interpretation[_]: Effect](
    statusUpdatesRunner:                StatusUpdatesRunner[Interpretation],
    awaitingTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge:        LabeledGauge[Interpretation, projects.Path],
    awaitingTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    underTriplesTransformationGauge:    LabeledGauge[Interpretation, projects.Path],
    logger:                             Logger[Interpretation]
)(implicit ME:                          MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import ch.datascience.controllers.InfoMessage._
  import ch.datascience.controllers.{ErrorMessage, InfoMessage}
  import org.http4s.circe._
  import org.http4s.{EntityDecoder, Request, Response}
  import statusUpdatesRunner.run

  def changeStatus(eventId: CompoundEventId,
                   request: Request[Interpretation]
  ): Interpretation[Response[Interpretation]] = {
    for {
      command      <- request.as[ChangeStatusCommand[Interpretation]](ME, findDecoder(eventId)) recoverWith badRequest
      updateResult <- run(command)
      response     <- updateResult.asHttpResponse
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[ChangeStatusCommand[Interpretation]]] = {
    case NonFatal(exception) => ME.raiseError(BadRequestError(exception))
  }

  private implicit class ResultOps(result: UpdateResult) {
    lazy val asHttpResponse: Interpretation[Response[Interpretation]] = result match {
      case UpdateResult.Updated  => Ok(InfoMessage("Event status updated"))
      case UpdateResult.NotFound => NotFound(InfoMessage("Event not found"))
      case UpdateResult.Conflict => Conflict(InfoMessage("Event status cannot be updated"))
      case UpdateResult.Failure(message) =>
        logger.error(message.value)
        InternalServerError(ErrorMessage(message.value))
    }
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Event status update failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private def findDecoder(
      eventId: CompoundEventId
  ): EntityDecoder[Interpretation, ChangeStatusCommand[Interpretation]] = {
    import ch.datascience.graph.model.events.EventStatus
    import ch.datascience.graph.model.events.EventStatus._
    import commands._
    import io.circe.{Decoder, HCursor}
    implicit val commandDecoder: Decoder[ChangeStatusCommand[Interpretation]] = (cursor: HCursor) =>
      for {
        status              <- cursor.downField("status").as[EventStatus]
        maybeMessage        <- cursor.downField("message").as[Option[EventMessage]]
        maybePayload        <- cursor.downField("payload").as[Option[EventPayload]]
        maybeSchemaVersion  <- cursor.downField("schemaVersion").as[Option[SchemaVersion]]
        maybeProcessingTime <- cursor.downField("processingTime").as[Option[EventProcessingTime]]
      } yield status match {
        case TriplesStore =>
          ToTriplesStore[Interpretation](eventId, underTriplesGenerationGauge, maybeProcessingTime)
        case New =>
          ToNew[Interpretation](eventId,
                                awaitingTriplesGenerationGauge,
                                underTriplesGenerationGauge,
                                maybeProcessingTime
          )
        case TriplesGenerated =>
          ToTriplesGenerated[Interpretation](
            eventId,
            maybePayload getOrElse (throw new Exception(s"$status status needs a payload")),
            maybeSchemaVersion getOrElse (throw new Exception(s"$status status needs a schemaVersion")),
            underTriplesGenerationGauge,
            awaitingTriplesTransformationGauge,
            maybeProcessingTime
          )
        case Skipped =>
          ToSkipped[Interpretation](eventId,
                                    maybeMessage getOrElse (throw new Exception(s"$status status needs a message")),
                                    underTriplesGenerationGauge,
                                    maybeProcessingTime
          )
        case GenerationRecoverableFailure =>
          ToGenerationRecoverableFailure[Interpretation](eventId,
                                                         maybeMessage,
                                                         awaitingTriplesGenerationGauge,
                                                         underTriplesGenerationGauge,
                                                         maybeProcessingTime
          )
        case GenerationNonRecoverableFailure =>
          ToGenerationNonRecoverableFailure[Interpretation](eventId,
                                                            maybeMessage,
                                                            underTriplesGenerationGauge,
                                                            maybeProcessingTime
          )
        case TransformationRecoverableFailure =>
          ToTransformationRecoverableFailure[Interpretation](eventId,
                                                             maybeMessage,
                                                             awaitingTriplesTransformationGauge,
                                                             underTriplesTransformationGauge,
                                                             maybeProcessingTime
          )
        case TransformationNonRecoverableFailure =>
          ToTransformationNonRecoverableFailure[Interpretation](eventId,
                                                                maybeMessage,
                                                                underTriplesTransformationGauge,
                                                                maybeProcessingTime
          )
        case other => throw new Exception(s"Transition to '$other' status unsupported")
      }

    jsonOf[Interpretation, ChangeStatusCommand[Interpretation]]
  }
}

object IOStatusChangeEndpoint {
  import cats.effect.IO

  def apply(
      transactor:                      DbTransactor[IO, EventLogDB],
      awaitTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
      underTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
      awaitingTransformationGauge:     LabeledGauge[IO, projects.Path],
      underTriplesTransformationGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:                LabeledHistogram[IO, SqlQuery.Name],
      logger:                          Logger[IO]
  )(implicit contextShift:             ContextShift[IO]): IO[StatusChangeEndpoint[IO]] =
    for {
      statusUpdatesRunner <- IOUpdateCommandsRunner(transactor, queriesExecTimes, logger)
    } yield new StatusChangeEndpoint(statusUpdatesRunner,
                                     awaitTriplesGenerationGauge,
                                     underTriplesGenerationGauge,
                                     awaitingTransformationGauge,
                                     underTriplesTransformationGauge,
                                     logger
    )
}
