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
import cats.data.Kleisli
import cats.effect.{ContextShift, Effect}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.http.ErrorMessage
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.commands._
import org.http4s.Request
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class StatusChangeEndpoint[Interpretation[_]: Effect](
    statusUpdatesRunner: StatusUpdatesRunner[Interpretation],
    commandFactories: Set[
      Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), Option[ChangeStatusCommand[Interpretation]]]
    ],
    logger:    Logger[Interpretation]
)(implicit ME: MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s.{Request, Response}
  import statusUpdatesRunner.run

  def changeStatus(eventId: CompoundEventId,
                   request: Request[Interpretation]
  ): Interpretation[Response[Interpretation]] =
    tryCommandFactory(eventId, request, commandFactories.toList)

  private def tryCommandFactory(
      eventId: CompoundEventId,
      request: Request[Interpretation],
      commandFactories: List[
        Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), Option[ChangeStatusCommand[Interpretation]]]
      ]
  ): Interpretation[Response[Interpretation]] = commandFactories match {
    case Nil => BadRequest(ErrorMessage("Invalid event"))
    case head :: tail =>
      head(eventId -> request).flatMap {
        case Some(command) => run(command).flatMap(_.asHttpResponse) recoverWith httpResponse
        case None          => tryCommandFactory(eventId, request, tail)
      }
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
                                       ToTriplesStore.factory(underTriplesGenerationGauge),
                                       ToNew.factory(awaitingTriplesGenerationGauge, underTriplesGenerationGauge),
                                       ToTriplesGenerated.factory(underTriplesGenerationGauge,
                                                                  awaitingTriplesTransformationGauge
                                       ),
                                       ToSkipped.factory(underTriplesGenerationGauge),
                                       ToGenerationNonRecoverableFailure.factory(underTriplesGenerationGauge),
                                       ToGenerationRecoverableFailure.factory(awaitingTriplesGenerationGauge,
                                                                              underTriplesGenerationGauge
                                       ),
                                       ToTransformationNonRecoverableFailure.factory(underTriplesTransformationGauge),
                                       ToTransformationRecoverableFailure.factory(awaitingTriplesTransformationGauge,
                                                                                  underTriplesTransformationGauge
                                       )
                                     ),
                                     logger
    )
}
