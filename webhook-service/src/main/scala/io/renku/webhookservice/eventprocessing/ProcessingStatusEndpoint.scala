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

package io.renku.webhookservice.eventprocessing

import cats.MonadThrow
import cats.data.OptionT
import cats.effect._
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import io.renku.http.ErrorMessage._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus
import io.renku.webhookservice.hookvalidation
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.{HookValidationResult, NoAccessTokenException}
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait ProcessingStatusEndpoint[F[_]] {
  def fetchProcessingStatus(projectId: Id): F[Response[F]]
}

class ProcessingStatusEndpointImpl[F[_]: MonadThrow: Logger](
    hookValidator:           HookValidator[F],
    processingStatusFetcher: ProcessingStatusFetcher[F],
    executionTimeRecorder:   ExecutionTimeRecorder[F]
) extends Http4sDsl[F]
    with ProcessingStatusEndpoint[F] {

  import HookValidationResult._
  import ProcessingStatusEndpointImpl._
  import executionTimeRecorder._

  def fetchProcessingStatus(projectId: Id): F[Response[F]] = measureExecutionTime {
    {
      for {
        _        <- validateHook(projectId)
        response <- findStatus(projectId)
      } yield response
    }.getOrElseF(NotFound(InfoMessage(s"Progress status for project '$projectId' not found")))
      .recoverWith(httpResponse(projectId))
  } map logExecutionTime(withMessage = s"Finding progress status for project '$projectId' finished")

  private def validateHook(projectId: Id): OptionT[F, Unit] = OptionT {
    hookValidator.validateHook(projectId, maybeAccessToken = None) map hookMissingToNone recover noAccessTokenToNone
  }

  private def findStatus(projectId: Id): OptionT[F, Response[F]] = OptionT.liftF {
    processingStatusFetcher
      .fetchProcessingStatus(projectId)
      .semiflatMap(processingStatus => Ok(processingStatus.asJson))
      .getOrElseF(Ok(zeroProcessingStatusJson))
  }

  private lazy val hookMissingToNone: HookValidationResult => Option[Unit] = {
    case HookExists => Some(())
    case _          => None
  }

  private lazy val noAccessTokenToNone: PartialFunction[Throwable, Option[Unit]] = { case NoAccessTokenException(_) =>
    None
  }

  private def httpResponse(
      projectId: projects.Id
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Finding progress status for project '$projectId' failed") >>
      InternalServerError(ErrorMessage(exception))
  }
}

private object ProcessingStatusEndpointImpl {

  implicit val processingStatusEncoder: Encoder[ProcessingStatus] = { case ProcessingStatus(done, total, progress) =>
    json"""
      {
       "done": ${done.value},
       "total": ${total.value},
       "progress": ${progress.value}
      }"""
  }

  val zeroProcessingStatusJson: Json =
    json"""
      {
       "done": ${0},
       "total": ${0}
      }"""
}

object ProcessingStatusEndpoint {
  def apply[F[_]: Async: Logger](
      projectHookUrl:        ProjectHookUrl,
      gitLabThrottler:       Throttler[F, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[F]
  ): F[ProcessingStatusEndpoint[F]] = for {
    fetcher       <- ProcessingStatusFetcher[F]
    hookValidator <- hookvalidation.HookValidator(projectHookUrl, gitLabThrottler)
  } yield new ProcessingStatusEndpointImpl[F](hookValidator, fetcher, executionTimeRecorder)
}
