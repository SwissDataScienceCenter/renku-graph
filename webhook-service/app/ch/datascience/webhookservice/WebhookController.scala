package ch.datascience.webhookservice

import akka.stream.QueueOfferResult
import ch.datascience.webhookservice.queue.PushEventQueue
import javax.inject.{ Inject, Singleton }
import play.api.libs.json.{ JsError, JsSuccess }
import play.api.mvc.{ AbstractController, ControllerComponents }
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

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

  import play.api.libs.functional.syntax._
  import play.api.libs.json.Reads._
  import play.api.libs.json._

  private implicit val gitRepositoryUrlReads: Reads[GitRepositoryUrl] = Reads[GitRepositoryUrl] {
    case JsString( value ) => Try( GitRepositoryUrl( value ) ).fold(
      error => JsError( error.getMessage ),
      value => JsSuccess( value )
    )
    case other => JsError( s"$other is not a valid GitRepositoryUrl" )
  }

  private implicit val checkoutShaReads: Reads[CheckoutSha] = Reads[CheckoutSha] {
    case JsString( value ) => Try( CheckoutSha( value ) ).fold(
      error => JsError( error.getMessage ),
      value => JsSuccess( value )
    )
    case other => JsError( s"$other is not a valid CheckoutSha" )
  }

  private implicit val projectNameReads: Reads[ProjectName] = Reads[ProjectName] {
    case JsString( value ) => Try( ProjectName( value ) ).fold(
      error => JsError( error.getMessage ),
      value => JsSuccess( value )
    )
    case other => JsError( s"$other is not a valid ProjectName" )
  }

  private[webhookservice] implicit val pushEventReads: Reads[PushEvent] = (
    ( __ \ "checkout_sha" ).read[CheckoutSha] and
    ( __ \ "repository" \ "git_http_url" ).read[GitRepositoryUrl] and
    ( __ \ "project" \ "name" ).read[ProjectName]
  )( PushEvent.apply _ )
}
