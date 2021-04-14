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

package io.renku.eventlog.processingstatus

import cats.MonadError
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class ProcessingStatusEndpoint[Interpretation[_]](
    processingStatusFinder: ProcessingStatusFinder[Interpretation],
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import cats.syntax.all._
  import ch.datascience.http.ErrorMessage._
  import ch.datascience.http.InfoMessage
  import ch.datascience.graph.model.projects
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Response
  import org.http4s.circe._
  import processingStatusFinder._

  def findProcessingStatus(projectId: projects.Id): Interpretation[Response[Interpretation]] = {
    for {
      maybeProcessingStatus <- fetchStatus(projectId).value
      response              <- maybeProcessingStatus.toResponse
    } yield response
  } recoverWith internalServerError(projectId)

  private implicit lazy val processingStatusEncoder: Encoder[ProcessingStatus] = Encoder.instance[ProcessingStatus] {
    case ProcessingStatus(done, total, progress) => json"""{
      "done":     ${done.value},
      "total":    ${total.value},
      "progress": ${progress.value}
    }"""
  }

  private implicit class StatusOps(maybeProcessingStatus: Option[ProcessingStatus]) {
    lazy val toResponse: Interpretation[Response[Interpretation]] =
      maybeProcessingStatus.fold {
        NotFound(InfoMessage("No processing status found").asJson)
      } { status =>
        Ok(status.asJson)
      }
  }

  private def internalServerError(
      projectId: projects.Id
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding processing status for project $projectId failed")
    logger.error(exception)(errorMessage.value)
    InternalServerError(errorMessage)
  }
}

object IOProcessingStatusEndpoint {

  import cats.effect.{ContextShift, IO}
  import io.renku.eventlog.EventLogDB

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[ProcessingStatusEndpoint[IO]] =
    for {
      statusFinder <- IOProcessingStatusFinder(transactor, queriesExecTimes)
    } yield new ProcessingStatusEndpoint[IO](statusFinder, logger)
}
