/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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
import ch.datascience.webhookservice.queues.pushevent._
import javax.inject.{ Inject, Singleton }
import play.api.libs.json.{ JsError, JsSuccess }
import play.api.mvc.{ AbstractController, ControllerComponents }
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class WebhookController(
    cc:             ControllerComponents,
    logger:         LoggerLike,
    pushEventQueue: PushEventQueue
) extends AbstractController( cc ) {

  @Inject def this( cc: ControllerComponents, pushEventQueue: PushEventQueue ) = this( cc, Logger, pushEventQueue )

  private implicit val executionContext: ExecutionContext = cc.executionContext

  import WebhookController.pushEventReads

  val processWebhookEvent = Action.async( cc.parsers.json ) { implicit request =>
    request.body.validate[PushEvent] match {
      case jsError@JsError( _ ) =>
        val errorResponse = ErrorResponse( jsError )
        logger.error( errorResponse.toString )
        Future.successful( BadRequest( errorResponse.toJson ) )
      case JsSuccess( pushEvent, _ ) =>
        pushEventQueue
          .offer( pushEvent )
          .map {
            case QueueOfferResult.Enqueued ⇒
              logger.info( s"'$pushEvent' enqueued" )
              Accepted
            case other ⇒
              val errorResponse = ErrorResponse( s"'$pushEvent' enqueueing problem: $other" )
              logger.error( errorResponse.toString )
              InternalServerError( errorResponse.toJson )
          }
    }
  }
}

object WebhookController {

  import ch.datascience.tinytypes.json._
  import play.api.libs.functional.syntax._
  import play.api.libs.json.Reads._
  import play.api.libs.json._

  private implicit val gitRepositoryUrlReads: Reads[GitRepositoryUrl] = TinyTypeReads( GitRepositoryUrl.apply )
  private implicit val checkoutShaReads: Reads[CheckoutSha] = TinyTypeReads( CheckoutSha.apply )
  private implicit val projectNameReads: Reads[ProjectName] = TinyTypeReads( ProjectName.apply )

  private[webhookservice] implicit val pushEventReads: Reads[PushEvent] = (
    ( __ \ "checkout_sha" ).read[CheckoutSha] and
    ( __ \ "repository" \ "git_http_url" ).read[GitRepositoryUrl] and
    ( __ \ "project" \ "name" ).read[ProjectName]
  )( PushEvent.apply _ )
}
