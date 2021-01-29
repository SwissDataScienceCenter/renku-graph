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

package ch.datascience.webhookservice.eventprocessing

import cats.MonadError
import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.http.ErrorMessage._
import ch.datascience.http.InfoMessage
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.eventprocessing.startcommit.{CommitToEventLog, IOCommitToEventLog}
import ch.datascience.webhookservice.model.HookToken
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, HCursor}
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.util.CaseInsensitiveString
import org.http4s.{EntityDecoder, Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait HookEventEndpoint[Interpretation[_]] {
  def processPushEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class HookEventEndpointImpl[Interpretation[_]: Effect](
    hookTokenCrypto:  HookTokenCrypto[Interpretation],
    commitToEventLog: CommitToEventLog[Interpretation],
    logger:           Logger[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation]
    with HookEventEndpoint[Interpretation] {

  import HookEventEndpoint._
  import commitToEventLog._
  import hookTokenCrypto._

  def processPushEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      startCommit <- request.as[StartCommit] recoverWith badRequest
      authToken   <- findHookToken(request)
      hookToken   <- decrypt(authToken) recoverWith unauthorizedException
      _           <- validate(hookToken, startCommit)
      _           <- storeCommitsInEventLog(startCommit)
      response    <- Accepted(InfoMessage("Event accepted"))
    } yield response
  } recoverWith httpResponse

  private implicit lazy val startCommitEntityDecoder: EntityDecoder[Interpretation, StartCommit] =
    jsonOf[Interpretation, StartCommit]

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[StartCommit]] = { case NonFatal(exception) =>
    ME.raiseError(BadRequestError(exception))
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private def findHookToken(request: Request[Interpretation]): Interpretation[SerializedHookToken] = ME.fromEither {
    request.headers.get(CaseInsensitiveString("X-Gitlab-Token")) match {
      case None           => Left(UnauthorizedException)
      case Some(rawToken) => SerializedHookToken.from(rawToken.value).leftMap(_ => UnauthorizedException)
    }
  }

  private lazy val unauthorizedException: PartialFunction[Throwable, Interpretation[HookToken]] = { case NonFatal(_) =>
    ME.raiseError(UnauthorizedException)
  }

  private def validate(hookToken: HookToken, startCommit: StartCommit): Interpretation[Unit] = ME.fromEither {
    if (hookToken.projectId == startCommit.project.id) Right(())
    else Left(UnauthorizedException)
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case ex @ UnauthorizedException =>
      Response[Interpretation](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex))
        .pure[Interpretation]
    case NonFatal(exception) =>
      logger.error(exception)(exception.getMessage)
      InternalServerError(ErrorMessage(exception))

  }
}

private object HookEventEndpoint {
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private implicit val projectDecoder: Decoder[Project] = (cursor: HCursor) => {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._
    for {
      id   <- cursor.downField("id").as[Id]
      path <- cursor.downField("path_with_namespace").as[Path]
    } yield Project(id, path)
  }

  implicit val pushEventDecoder: Decoder[StartCommit] = (cursor: HCursor) =>
    for {
      commitTo <- cursor.downField("after").as[CommitId]
      project  <- cursor.downField("project").as[Project]
    } yield StartCommit(commitTo, project)
}

object IOHookEventEndpoint {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      hookTokenCrypto:       HookTokenCrypto[IO],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[HookEventEndpoint[IO]] =
    for {
      commitToEventLog <- IOCommitToEventLog(gitLabThrottler, executionTimeRecorder, logger)
    } yield new HookEventEndpointImpl[IO](hookTokenCrypto, commitToEventLog, logger)
}
