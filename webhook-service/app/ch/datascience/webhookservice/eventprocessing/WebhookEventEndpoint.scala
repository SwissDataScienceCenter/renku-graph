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

import akka.stream.QueueOfferResult
import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events._
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.crypto.{HookTokenCrypto, IOHookTokenCrypto}
import ch.datascience.webhookservice.crypto.HookTokenCrypto.HookAuthToken
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.queues.pushevent.{PushEvent, PushEventQueue}
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

@Singleton
class WebhookEventEndpoint(
    cc:              ControllerComponents,
    logger:          Logger[IO],
    hookTokenCrypto: HookTokenCrypto[IO],
    pushEventQueue:  PushEventQueue
) extends AbstractController(cc) {

  @Inject def this(
      cc:              ControllerComponents,
      pushEventQueue:  PushEventQueue,
      logger:          IOLogger,
      hookTokenCrypto: IOHookTokenCrypto
  ) = this(cc, logger, hookTokenCrypto, pushEventQueue)

  private implicit val executionContext: ExecutionContext = defaultExecutionContext

  import WebhookEventEndpoint._
  import hookTokenCrypto._

  val processPushEvent: Action[PushEvent] = Action.async(parse.json[PushEvent]) { implicit request =>
    (for {
      authToken      <- findAuthToken(request)
      decryptedToken <- decrypt(authToken) recoverWith unauthorizedException
      _              <- validate(decryptedToken, request.body)
      result         <- process(pushEvent = request.body)
      _              <- logger.info(s"'${request.body}' enqueued")
    } yield result)
      .recover(logError andThen mapToResult)
      .unsafeToFuture()
  }

  private def findAuthToken(request: Request[_]): IO[HookAuthToken] = IO.fromEither {
    request.headers.get("X-Gitlab-Token") match {
      case None           => Left(UnauthorizedException)
      case Some(rawToken) => HookAuthToken.from(rawToken).leftMap(_ => UnauthorizedException)
    }
  }

  private lazy val unauthorizedException: PartialFunction[Throwable, IO[String]] = {
    case NonFatal(_) =>
      IO.raiseError(UnauthorizedException)
  }

  private def validate(decryptedToken: String, pushEvent: PushEvent): IO[Unit] = IO.fromEither {
    if (decryptedToken == pushEvent.project.id.toString) Right(())
    else Left(UnauthorizedException)
  }

  private def process(pushEvent: PushEvent): IO[Result] =
    IO.fromFuture(IO(pushEventQueue.offer(pushEvent)))
      .flatMap {
        case QueueOfferResult.Enqueued => IO.pure(Accepted)
        case other                     => IO.raiseError(new RuntimeException(s"'$pushEvent' enqueueing problem: $other"))
      }

  private lazy val mapToResult: Throwable => Result = {
    case ex @ UnauthorizedException => Unauthorized(ErrorMessage(ex.getMessage).toJson)
    case NonFatal(exception)        => InternalServerError(ErrorMessage(exception.getMessage).toJson)
  }

  private lazy val logError: PartialFunction[Throwable, Throwable] = {
    case ex @ UnauthorizedException => ex
    case NonFatal(exception) =>
      logger.error(exception.getMessage)
      exception
  }
}

object WebhookEventEndpoint {

  import play.api.libs.functional.syntax._
  import play.api.libs.json.Reads._
  import play.api.libs.json._

  private implicit val projectReads: Reads[Project] = (
    (__ \ "id").read[ProjectId] and
      (__ \ "path_with_namespace").read[ProjectPath]
  )(Project.apply _)

  private[webhookservice] implicit val pushEventReads: Reads[PushEvent] = (
    (__ \ "before").read[CommitId] and
      (__ \ "after").read[CommitId] and
      (__ \ "user_id").read[UserId] and
      (__ \ "user_username").read[Username] and
      (__ \ "user_email").read[Email] and
      (__ \ "project").read[Project]
  )(toPushEvent _)

  private def toPushEvent(
      before:   CommitId,
      after:    CommitId,
      userId:   UserId,
      username: Username,
      email:    Email,
      project:  Project
  ): PushEvent = PushEvent(
    before,
    after,
    PushUser(userId, username, email),
    project
  )
}
