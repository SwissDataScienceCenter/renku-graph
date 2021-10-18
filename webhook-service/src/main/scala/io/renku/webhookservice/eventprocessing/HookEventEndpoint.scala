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

package io.renku.webhookservice.eventprocessing

import cats.MonadError
import cats.effect._
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.ErrorMessage._
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.webhookservice.CommitSyncRequestSender
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.model.{CommitSyncRequest, HookToken, Project}
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.util.CaseInsensitiveString
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait HookEventEndpoint[Interpretation[_]] {
  def processPushEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class HookEventEndpointImpl[Interpretation[_]: MonadError[*[_], Throwable]](
    hookTokenCrypto:         HookTokenCrypto[Interpretation],
    commitSyncRequestSender: CommitSyncRequestSender[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends Http4sDsl[Interpretation]
    with HookEventEndpoint[Interpretation] {

  import HookEventEndpoint._
  import commitSyncRequestSender._
  import hookTokenCrypto._

  def processPushEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      pushEvent <- request.as[(CommitId, CommitSyncRequest)] recoverWith badRequest
      authToken <- findHookToken(request)
      hookToken <- decrypt(authToken) recoverWith unauthorizedException
      _         <- validate(hookToken, pushEvent._2)
      _         <- contextShift.shift *> concurrent.start(sendCommitSyncRequest(pushEvent._2))
      _         <- logInfo(pushEvent)
      response  <- Accepted(InfoMessage("Event accepted"))
    } yield response
  } recoverWith httpResponse

  private implicit lazy val startCommitEntityDecoder: EntityDecoder[Interpretation, (CommitId, CommitSyncRequest)] =
    jsonOf[Interpretation, (CommitId, CommitSyncRequest)]

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[(CommitId, CommitSyncRequest)]] = {
    case NonFatal(exception) => BadRequestError(exception).raiseError[Interpretation, (CommitId, CommitSyncRequest)]
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private def findHookToken(request: Request[Interpretation]): Interpretation[SerializedHookToken] =
    request.headers.get(CaseInsensitiveString("X-Gitlab-Token")) match {
      case None => UnauthorizedException.raiseError[Interpretation, SerializedHookToken]
      case Some(rawToken) =>
        SerializedHookToken
          .from(rawToken.value)
          .fold(
            _ => UnauthorizedException.raiseError[Interpretation, SerializedHookToken],
            _.pure[Interpretation]
          )
    }

  private lazy val unauthorizedException: PartialFunction[Throwable, Interpretation[HookToken]] = { case NonFatal(_) =>
    UnauthorizedException.raiseError[Interpretation, HookToken]
  }

  private def validate(hookToken: HookToken, pushEvent: CommitSyncRequest): Interpretation[Unit] =
    if (hookToken.projectId == pushEvent.project.id) ().pure[Interpretation]
    else UnauthorizedException.raiseError[Interpretation, Unit]

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

  private lazy val logInfo: ((CommitId, CommitSyncRequest)) => Interpretation[Unit] = {
    case (commitId, CommitSyncRequest(project)) =>
      logger.info(
        s"Push event for eventId = $commitId, projectId = ${project.id}, projectPath = ${project.path} -> accepted"
      )
  }
}

private object HookEventEndpoint {

  private implicit val projectDecoder: Decoder[Project] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    for {
      id   <- cursor.downField("id").as[Id]
      path <- cursor.downField("path_with_namespace").as[Path]
    } yield Project(id, path)
  }

  implicit val pushEventDecoder: Decoder[(CommitId, CommitSyncRequest)] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    for {
      commitId <- cursor.downField("after").as[CommitId]
      project  <- cursor.downField("project").as[Project]
    } yield commitId -> CommitSyncRequest(project)
  }
}

object IOHookEventEndpoint {
  def apply(
      hookTokenCrypto: HookTokenCrypto[IO],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[HookEventEndpoint[IO]] = for {
    commitSyncRequestSender <- CommitSyncRequestSender(logger)
  } yield new HookEventEndpointImpl[IO](hookTokenCrypto, commitSyncRequestSender, logger)
}
