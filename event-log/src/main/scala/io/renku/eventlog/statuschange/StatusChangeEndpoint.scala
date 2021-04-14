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

import ChangeStatusRequest.EventOnlyRequest
import CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import cats.MonadError
import cats.data.EitherT.right
import cats.data.{EitherT, Kleisli}
import cats.effect.{ContextShift, Effect}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.http.ErrorMessage
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger
import io.circe.{Decoder, Json}
import io.renku.eventlog.statuschange.commands._
import io.renku.eventlog.{EventLogDB, EventMessage}
import org.http4s.dsl.Http4sDsl
import org.http4s.multipart.{Multipart, Part}

import scala.language.implicitConversions
import scala.util.control.NonFatal

class StatusChangeEndpoint[Interpretation[_]: Effect: MonadError[*[_], Throwable]](
    statusUpdatesRunner: StatusUpdatesRunner[Interpretation],
    commandFactories:    Set[Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult]],
    logger:              Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s.circe.jsonDecoder
  import org.http4s.{Request, Response}
  import statusUpdatesRunner.run

  def changeStatus(eventId: CompoundEventId,
                   request: Request[Interpretation]
  ): Interpretation[Response[Interpretation]] = {
    for {
      changeStatusRequest <- decodeRequest(eventId, request)
      response            <- right[Response[Interpretation]](tryCommandFactory(commandFactories.toList, changeStatusRequest))
    } yield response
  }.merge

  private def decodeRequest(eventId: CompoundEventId,
                            request: Request[Interpretation]
  ): EitherT[Interpretation, Response[Interpretation], ChangeStatusRequest] = EitherT {
    {
      for {
        multipart <- request.as[Multipart[Interpretation]]
        eventPart <-
          multipart.parts
            .find(_.name.contains("event"))
            .map(_.pure[Interpretation])
            .getOrElse(
              new Exception("No event part in change status payload").raiseError[Interpretation, Part[Interpretation]]
            )
        eventJson <- eventPart.as[Json]
        eventOnlyRequest <- eventJson
                              .as[EventOnlyRequest](requestDecoder(eventId))
                              .fold(_.raiseError[Interpretation, EventOnlyRequest], _.pure[Interpretation])
        changeStatusRequest <- createRequest(eventOnlyRequest)(multipart.parts.find(_.name.contains("payload")))
      } yield changeStatusRequest
    }.map(_.asRight[Response[Interpretation]]) recoverWith badRequest
  }

  private implicit def requestDecoder(eventId: CompoundEventId): Decoder[EventOnlyRequest] = { cursor =>
    for {
      status              <- cursor.downField("status").as[EventStatus]
      maybeProcessingTime <- cursor.downField("processingTime").as[Option[EventProcessingTime]]
      maybeMessage        <- cursor.downField("message").as[Option[EventMessage]]
    } yield EventOnlyRequest(eventId, status, maybeProcessingTime, maybeMessage)
  }

  private def createRequest(
      eventOnlyRequest: EventOnlyRequest
  ): Kleisli[Interpretation, Option[Part[Interpretation]], ChangeStatusRequest] = Kleisli {
    case Some(payloadPart) => payloadPart.as[String].map(payload => eventOnlyRequest.addPayload(payload))
    case None              => (eventOnlyRequest: ChangeStatusRequest).pure[Interpretation]
  }

  def badRequest: PartialFunction[Throwable, Interpretation[Either[Response[Interpretation], ChangeStatusRequest]]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Decoding status change request body failed")
      BadRequest(ErrorMessage("Malformed event or payload")).map(_.asLeft[ChangeStatusRequest])
  }

  private def tryCommandFactory(
      commandFactories:    List[Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult]],
      changeStatusRequest: ChangeStatusRequest
  ): Interpretation[Response[Interpretation]] = commandFactories match {
    case Nil => BadRequest(ErrorMessage("No event command found"))
    case headFactory :: otherFactories =>
      headFactory.run(changeStatusRequest).flatMap {
        case command: CommandFound[Interpretation] =>
          run(command.command).flatMap(_.asHttpResponse) recoverWith httpResponse
        case NotSupported              => tryCommandFactory(otherFactories, changeStatusRequest)
        case PayloadMalformed(message) => BadRequest(ErrorMessage(message))
      }
  }

  private implicit class ResultOps(result: UpdateResult) {
    lazy val asHttpResponse: Interpretation[Response[Interpretation]] = result match {
      case UpdateResult.Updated          => Ok(InfoMessage("Event status updated"))
      case UpdateResult.NotFound         => NotFound(InfoMessage("Event not found"))
      case UpdateResult.Failure(message) => InternalServerError(ErrorMessage(message.value))
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

}

object IOStatusChangeEndpoint {

  import cats.effect.IO

  def apply(
      transactor:                         DbTransactor[IO, EventLogDB],
      awaitingTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
      underTriplesGenerationGauge:        LabeledGauge[IO, projects.Path],
      awaitingTriplesTransformationGauge: LabeledGauge[IO, projects.Path],
      underTriplesTransformationGauge:    LabeledGauge[IO, projects.Path],
      queriesExecTimes:                   LabeledHistogram[IO, SqlQuery.Name],
      logger:                             Logger[IO]
  )(implicit contextShift:                ContextShift[IO]): IO[StatusChangeEndpoint[IO]] =
    for {
      statusUpdatesRunner <- IOUpdateCommandsRunner(transactor, queriesExecTimes, logger)
    } yield new StatusChangeEndpoint(statusUpdatesRunner,
                                     Set(
                                       ToTriplesStore.factory(underTriplesTransformationGauge),
                                       ToNew.factory(awaitingTriplesGenerationGauge, underTriplesGenerationGauge),
                                       ToTriplesGenerated.factory(transactor,
                                                                  underTriplesTransformationGauge,
                                                                  underTriplesGenerationGauge,
                                                                  awaitingTriplesTransformationGauge
                                       ),
                                       ToGenerationNonRecoverableFailure.factory(underTriplesGenerationGauge),
                                       ToGenerationRecoverableFailure.factory(awaitingTriplesGenerationGauge,
                                                                              underTriplesGenerationGauge
                                       ),
                                       ToTransformationNonRecoverableFailure.factory(
                                         underTriplesTransformationGauge
                                       ),
                                       ToTransformationRecoverableFailure.factory(awaitingTriplesTransformationGauge,
                                                                                  underTriplesTransformationGauge
                                       )
                                     ),
                                     logger
    )
}
