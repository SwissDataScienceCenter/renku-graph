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
import cats.data.EitherT
import cats.effect.{ContextShift, Effect}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.http.EventRequest.EventRequestContent
import ch.datascience.http.{ErrorMessage, MultipartRequest}
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, DecodingFailure}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import io.renku.eventlog.{EventLogDB, EventMessage, EventPayload, EventProcessingTime}
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class StatusChangeEndpoint[Interpretation[_]: Effect](
    statusUpdatesRunner:                StatusUpdatesRunner[Interpretation],
    awaitingTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge:        LabeledGauge[Interpretation, projects.Path],
    awaitingTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    underTriplesTransformationGauge:    LabeledGauge[Interpretation, projects.Path],
    logger:                             Logger[Interpretation]
)(implicit ME:                          MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation]
    with MultipartRequest {

  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s.{Request, Response}
  import statusUpdatesRunner.run

  def changeStatus(eventId: CompoundEventId,
                   request: Request[Interpretation]
  ): Interpretation[Response[Interpretation]] = {
    decodeRequestAndProcess(eventId, request)
  } recoverWith httpResponse

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
      eventId:      CompoundEventId,
      maybePayload: Option[EventPayload]
  ): Decoder[ChangeStatusCommand[Interpretation]] = {
    import ch.datascience.graph.model.events.EventStatus
    import ch.datascience.graph.model.events.EventStatus._
    import commands._
    import io.circe.HCursor
    (cursor: HCursor) =>
      {
        for {
          maybeStatus         <- cursor.downField("status").as[Option[EventStatus]]
          maybeMessage        <- cursor.downField("message").as[Option[EventMessage]]
          maybeSchemaVersion  <- cursor.downField("schemaVersion").as[Option[SchemaVersion]]
          maybeProcessingTime <- cursor.downField("processingTime").as[Option[EventProcessingTime]]
        } yield maybeStatus
          .map {
            case TriplesStore =>
              Right(ToTriplesStore[Interpretation](eventId, underTriplesGenerationGauge, maybeProcessingTime))
            case New =>
              Right(
                ToNew[Interpretation](eventId,
                                      awaitingTriplesGenerationGauge,
                                      underTriplesGenerationGauge,
                                      maybeProcessingTime
                )
              )
            case status @ TriplesGenerated =>
              (maybePayload, maybeSchemaVersion) match {
                case (Some(payload), Some(schemaVersion)) =>
                  Right(
                    ToTriplesGenerated[Interpretation](
                      eventId,
                      payload,
                      schemaVersion,
                      underTriplesGenerationGauge,
                      awaitingTriplesTransformationGauge,
                      maybeProcessingTime
                    )
                  )
                case (None, _) => Left(DecodingFailure(s"$status status needs a payload", Nil))
                case (_, None) => Left(DecodingFailure(s"$status status needs a schemaVersion", Nil))
              }

            case status @ Skipped =>
              maybeMessage match {
                case Some(message) =>
                  Right(ToSkipped[Interpretation](eventId, message, underTriplesGenerationGauge, maybeProcessingTime))
                case None => Left(DecodingFailure(s"$status status needs a message", Nil))
              }
            case GenerationRecoverableFailure =>
              Right(
                ToGenerationRecoverableFailure[Interpretation](eventId,
                                                               maybeMessage,
                                                               awaitingTriplesGenerationGauge,
                                                               underTriplesGenerationGauge,
                                                               maybeProcessingTime
                )
              )
            case GenerationNonRecoverableFailure =>
              Right(
                ToGenerationNonRecoverableFailure[Interpretation](eventId,
                                                                  maybeMessage,
                                                                  underTriplesGenerationGauge,
                                                                  maybeProcessingTime
                )
              )
            case TransformationRecoverableFailure =>
              Right(
                ToTransformationRecoverableFailure[Interpretation](eventId,
                                                                   maybeMessage,
                                                                   awaitingTriplesTransformationGauge,
                                                                   underTriplesTransformationGauge,
                                                                   maybeProcessingTime
                )
              )
            case TransformationNonRecoverableFailure =>
              Right(
                ToTransformationNonRecoverableFailure[Interpretation](eventId,
                                                                      maybeMessage,
                                                                      underTriplesTransformationGauge,
                                                                      maybeProcessingTime
                )
              )
            case other => Left(DecodingFailure(s"Transition to '$other' status unsupported", Nil))

          }
          .getOrElse(
            Left(DecodingFailure(s"Invalid message body: Could not decode JSON: ${cursor.value.toString}", Nil))
          )
      }.flatten
  }

  private def decodeRequestAndProcess(
      eventId: CompoundEventId,
      request: Request[Interpretation]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[Response[Interpretation]] = decodeAndProcessMultipart[Interpretation, EventRequestContent](
    request,
    content =>
      {
        for {
          requestContent <- content.leftSemiflatMap(error => httpResponse(BadRequestError(error)))
          command <- EitherT
                       .fromEither[Interpretation] {
                         requestContent.event.as[ChangeStatusCommand[Interpretation]](
                           findDecoder(eventId, requestContent.maybePayload.map(EventPayload.apply))
                         )
                       }
                       .leftSemiflatMap(e => httpResponse(BadRequestError(e)))
          response <- EitherT.liftF[Interpretation, Response[Interpretation], Response[Interpretation]](
                        run(command).flatMap(_.asHttpResponse)
                      )
        } yield response
      }.merge
  )

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
