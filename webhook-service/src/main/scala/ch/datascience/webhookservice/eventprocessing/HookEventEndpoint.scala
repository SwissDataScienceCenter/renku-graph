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
import ch.datascience.control.Throttler
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.model.events._
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.eventprocessing.pushevent.{IOPushEventSender, PushEventSender}
import ch.datascience.webhookservice.model.HookToken
import io.circe.{Decoder, HCursor}
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.util.CaseInsensitiveString
import org.http4s.{EntityDecoder, Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class HookEventEndpoint[Interpretation[_]: Effect](
    hookTokenCrypto: HookTokenCrypto[Interpretation],
    pushEventSender: PushEventSender[Interpretation]
)(implicit ME:       MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import HookEventEndpoint._
  import hookTokenCrypto._
  import pushEventSender._

  def processPushEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      pushEvent <- request.as[PushEvent] recoverWith badRequest
      authToken <- findHookToken(request)
      hookToken <- decrypt(authToken) recoverWith unauthorizedException
      _         <- validate(hookToken, pushEvent)
      _         <- storeCommitsInEventLog(pushEvent)
      response  <- Accepted(InfoMessage("Event accepted"))
    } yield response
  } recoverWith httpResponse

  private implicit lazy val pushEventEntityDecoder: EntityDecoder[Interpretation, PushEvent] =
    jsonOf[Interpretation, PushEvent]

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[PushEvent]] = {
    case NonFatal(exception) =>
      ME.raiseError(BadRequestError(exception))
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private def findHookToken(request: Request[Interpretation]): Interpretation[SerializedHookToken] = ME.fromEither {
    request.headers.get(CaseInsensitiveString("X-Gitlab-Token")) match {
      case None           => Left(UnauthorizedException)
      case Some(rawToken) => SerializedHookToken.from(rawToken.value).leftMap(_ => UnauthorizedException)
    }
  }

  private lazy val unauthorizedException: PartialFunction[Throwable, Interpretation[HookToken]] = {
    case NonFatal(_) =>
      ME.raiseError(UnauthorizedException)
  }

  private def validate(hookToken: HookToken, pushEvent: PushEvent): Interpretation[Unit] = ME.fromEither {
    if (hookToken.projectId == pushEvent.project.id) Right(())
    else Left(UnauthorizedException)
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) =>
      BadRequest(ErrorMessage(exception.getMessage))
    case ex @ UnauthorizedException =>
      Response[Interpretation](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex.getMessage))
        .pure[Interpretation]
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception.getMessage))
  }
}

private object HookEventEndpoint {

  import io.circe.DecodingFailure

  private implicit val projectDecoder: Decoder[Project] = (cursor: HCursor) => {
    for {
      id   <- cursor.downField("id").as[ProjectId]
      path <- cursor.downField("path_with_namespace").as[ProjectPath]
    } yield Project(id, path)
  }

  implicit val pushEventDecoder: Decoder[PushEvent] = (cursor: HCursor) => {
    for {
      maybeCommitFrom <- cursor.downField("before").as[Option[CommitId]]
      commitTo        <- cursor.downField("after").as[CommitId]
      userId          <- cursor.downField("user_id").as[UserId]
      username        <- cursor.downField("user_username").as[Username]
      maybeEmail      <- cursor.downField("user_email").as[Option[String]].flatMap(emptyToNone)
      project         <- cursor.downField("project").as[Project]
    } yield
      PushEvent(
        maybeCommitFrom,
        commitTo,
        PushUser(userId, username, maybeEmail),
        project
      )
  }

  private lazy val emptyToNone: Option[String] => Either[DecodingFailure, Option[Email]] = {
    case Some("") => Right(None)
    case None     => Right(None)
    case Some(nonBlankEmail) =>
      Email
        .from(nonBlankEmail)
        .map(Some(_))
        .leftMap(exception => DecodingFailure.apply(exception.getMessage, Nil))
  }
}

class IOHookEventEndpoint(
    gitLabThrottler: Throttler[IO, GitLab]
)(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    clock:                     Clock[IO]
) extends HookEventEndpoint[IO](HookTokenCrypto[IO], new IOPushEventSender(gitLabThrottler))
