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

package ch.datascience.webhookservice

import akka.stream.QueueOfferResult
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events._
import ch.datascience.webhookservice.queues.pushevent.{PushEvent, PushEventQueue}
import javax.inject.{Inject, Singleton}
import play.api.mvc._
import play.api.{Logger, LoggerLike}

import scala.concurrent.ExecutionContext

@Singleton
class PushEventConsumer(
    cc:             ControllerComponents,
    logger:         LoggerLike,
    pushEventQueue: PushEventQueue
) extends AbstractController(cc) {

  @Inject def this(
      cc:             ControllerComponents,
      pushEventQueue: PushEventQueue
  ) = this(cc, Logger, pushEventQueue)

  private implicit val executionContext: ExecutionContext = defaultExecutionContext

  import PushEventConsumer._

  val processPushEvent: Action[PushEvent] = Action.async(parse.json[PushEvent]) { implicit request =>
    pushEventQueue
      .offer(request.body)
      .map {
        case QueueOfferResult.Enqueued ⇒
          logger.info(s"'${request.body}' enqueued")
          Accepted
        case other ⇒
          val errorResponse = ErrorMessage(s"'${request.body}' enqueueing problem: $other")
          logger.error(errorResponse.toString)
          InternalServerError(errorResponse.toJson)
      }
  }
}

object PushEventConsumer {

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
