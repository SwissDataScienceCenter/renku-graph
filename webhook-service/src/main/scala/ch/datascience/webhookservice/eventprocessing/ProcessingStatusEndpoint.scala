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
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events._
import ch.datascience.graph.tokenrepository.TokenRepositoryUrl
import ch.datascience.webhookservice.config.GitLab
import ch.datascience.webhookservice.hookvalidation.HookValidator.{HookValidationResult, NoAccessTokenException}
import ch.datascience.webhookservice.hookvalidation.{HookValidator, IOHookValidator}
import ch.datascience.webhookservice.project.ProjectHookUrl
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
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

  def fetchProcessingStatus(projectId: ProjectId): Interpretation[Response[Interpretation]] = {
    for {
      _        <- validateHook(projectId)
      response <- findStatus(projectId)
    } yield response
  } getOrElseF NotFound(InfoMessage(s"Progress status for project '$projectId' not found")) recoverWith httpResponse

  private def validateHook(projectId: ProjectId): OptionT[Interpretation, Unit] = OptionT {
    hookValidator.validateHook(projectId, maybeAccessToken = None) map hookMissingToNone recover noAccessTokenToNone
  }

  private def findStatus(projectId: ProjectId): OptionT[Interpretation, Response[Interpretation]] = OptionT.liftF {
    fetchStatus(projectId)
      .semiflatMap(processingStatus => Ok(processingStatus.asJson))
      .getOrElseF(Ok(zeroProcessingStatusJson))
  }

  private lazy val hookMissingToNone: HookValidationResult => Option[Unit] = {
    case HookExists => Some(())
    case _          => None
  }

  private lazy val noAccessTokenToNone: PartialFunction[Throwable, Option[Unit]] = {
    case NoAccessTokenException(_) => None
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception))
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

  val zeroProcessingStatusJson: Json = json"""
      {
       "done": ${0},
       "total": ${0}
      }"""
}

class IOProcessingStatusEndpoint(
    transactor:              DbTransactor[IO, EventLogDB],
    tokenRepositoryUrl:      TokenRepositoryUrl,
    projectHookUrl:          ProjectHookUrl,
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], clock: Clock[IO], timer: Timer[IO])
    extends ProcessingStatusEndpoint[IO](
      new IOHookValidator(tokenRepositoryUrl, projectHookUrl, gitLabUrl, gitLabThrottler),
      new IOEventLogProcessingStatus(transactor))
