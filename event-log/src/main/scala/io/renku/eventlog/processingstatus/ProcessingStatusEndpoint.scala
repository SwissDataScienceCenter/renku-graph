/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.effect.MonadCancelThrow
import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.metrics.LabeledHistogram
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait ProcessingStatusEndpoint[F[_]] {
  def findProcessingStatus(projectId: projects.Id): F[Response[F]]
}

class ProcessingStatusEndpointImpl[F[_]: MonadThrow: Logger](
    processingStatusFinder: ProcessingStatusFinder[F]
) extends Http4sDsl[F]
    with ProcessingStatusEndpoint[F] {

  import cats.syntax.all._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import io.renku.graph.model.projects
  import io.renku.http.ErrorMessage._
  import io.renku.http.InfoMessage
  import org.http4s.Response
  import org.http4s.circe._
  import processingStatusFinder._

  override def findProcessingStatus(projectId: projects.Id): F[Response[F]] = {
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
    lazy val toResponse: F[Response[F]] =
      maybeProcessingStatus.fold {
        NotFound(InfoMessage("No processing status found").asJson)
      } { status =>
        Ok(status.asJson)
      }
  }

  private def internalServerError(
      projectId: projects.Id
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Finding processing status for project $projectId failed")
    Logger[F].error(exception)(errorMessage.value)
    InternalServerError(errorMessage)
  }
}

object ProcessingStatusEndpoint {

  import io.renku.eventlog.EventLogDB

  def apply[F[_]: MonadCancelThrow: Async: Logger](
      sessionResource:  SessionResource[F, EventLogDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[ProcessingStatusEndpoint[F]] = for {
    statusFinder <- ProcessingStatusFinder(sessionResource, queriesExecTimes)
  } yield new ProcessingStatusEndpointImpl[F](statusFinder)
}
