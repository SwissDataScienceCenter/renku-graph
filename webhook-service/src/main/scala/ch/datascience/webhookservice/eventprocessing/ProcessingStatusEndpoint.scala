/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.{EventLogProcessingStatus, IOEventLogProcessingStatus, ProcessingStatus}
import ch.datascience.graph.model.events._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class ProcessingStatusEndpoint[Interpretation[_]: Effect](
    eventsProcessingStatus: EventLogProcessingStatus[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import eventsProcessingStatus._
  import ProcessingStatusEndpoint._

  def fetchProcessingStatus(projectId: ProjectId): Interpretation[Response[Interpretation]] = {
    for {
      maybeProcessingStatus <- fetchStatus(projectId)
      response <- maybeProcessingStatus match {
                   case Some(processingStatus) => Ok(processingStatus.asJson)
                   case None                   => NotFound(InfoMessage(s"Project: $projectId not found"))
                 }
    } yield response
  } recoverWith httpResponse

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception.getMessage))
  }
}

private object ProcessingStatusEndpoint {

  implicit val processingStatusEncoder: Encoder[ProcessingStatus] = {
    case ProcessingStatus(done, total, progress) => json"""
      {
       "done": ${done.value},
       "total": ${total.value},
       "progress": ${progress.value}
      }"""
  }
}

class IOProcessingStatusEndpoint(
    transactor:              DbTransactor[IO, EventLogDB]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], clock: Clock[IO])
    extends ProcessingStatusEndpoint[IO](new IOEventLogProcessingStatus(transactor))
