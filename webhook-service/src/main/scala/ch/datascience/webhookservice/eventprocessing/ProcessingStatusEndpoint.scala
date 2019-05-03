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
import cats.data.OptionT
import cats.effect._
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.{EventLogProcessingStatus, IOEventLogProcessingStatus, ProcessingStatus}
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.model.events._
<<<<<<< HEAD
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
=======
import ch.datascience.webhookservice.hookvalidation.HookValidator.{HookValidationResult, NoAccessTokenException}
>>>>>>> 2af18b6... fix: status endpoint to return NOT_FOUND when no hook for a project
import ch.datascience.webhookservice.hookvalidation.{HookValidator, IOHookValidator}
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
    hookValidator:          HookValidator[Interpretation],
    eventsProcessingStatus: EventLogProcessingStatus[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import HookValidationResult._
  import ProcessingStatusEndpoint._
  import eventsProcessingStatus._
  import hookValidator._

  def fetchProcessingStatus(projectId: ProjectId): Interpretation[Response[Interpretation]] = {
    for {
<<<<<<< HEAD
      _        <- OptionT(validateHook(projectId, maybeAccessToken = None) map hookDoesNotExistToNone)
      response <- fetchStatus(projectId) semiflatMap (processingStatus => Ok(processingStatus.asJson))
    } yield response
  } getOrElseF NotFound(InfoMessage(s"Project: $projectId not found")) recoverWith httpResponse

  private lazy val hookDoesNotExistToNone: HookValidationResult => Option[Unit] = {
    case HookExists => Some(())
    case _          => None
  }
=======
      _        <- OptionT(validateHook(projectId, maybeAccessToken = None) map hookMissingToNone recover noAccessTokenToNone)
      response <- fetchStatus(projectId) semiflatMap (processingStatus => Ok(processingStatus.asJson))
    } yield response
  } getOrElseF NotFound(InfoMessage(s"Progress status for project '$projectId' not found")) recoverWith httpResponse

  private lazy val hookMissingToNone: HookValidationResult => Option[Unit] = {
    case HookExists => Some(())
    case _          => None
  }

  private lazy val noAccessTokenToNone: PartialFunction[Throwable, Option[Unit]] = {
    case NoAccessTokenException(_) => None
  }
>>>>>>> 2af18b6... fix: status endpoint to return NOT_FOUND when no hook for a project

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
    transactor:              DbTransactor[IO, EventLogDB],
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], clock: Clock[IO])
    extends ProcessingStatusEndpoint[IO](new IOHookValidator(gitLabThrottler),
                                         new IOEventLogProcessingStatus(transactor))
