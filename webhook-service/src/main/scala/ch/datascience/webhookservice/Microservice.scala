package ch.datascience.webhookservice

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.http.scaladsl.server.RejectionHandler
import akka.stream.ActorMaterializer
import ch.datascience.config.ConfigOps.Implicits._
import ch.datascience.webhookservice.queue.PushEventQueue
import com.typesafe.config.ConfigFactory
import spray.json.{JsObject, JsString}

import scala.concurrent.ExecutionContextExecutor

object Microservice extends App {
  private implicit val system: ActorSystem = ActorSystem()
  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val config = ConfigFactory.load()
  private val logger = Logging(system, getClass)
  private val pushEventQueue = PushEventQueue(config, logger)
  private val webhookEndpoint = WebhookEndpoint(logger, pushEventQueue)

  private implicit val rejectionHandler: RejectionHandler =
    RejectionHandler
      .default
      .mapRejectionResponse {
        case response@HttpResponse(_, _, entity: HttpEntity.Strict, _) =>
          val message = entity.data.utf8String.replaceAll("\"", """\"""")
          response.copy(
            entity = HttpEntity(
              `application/json`,
              JsObject("rejection" -> JsString(message)).prettyPrint
            )
          )
        case response                                                  => response
      }

  Http().bindAndHandle(
    handler = webhookEndpoint.routes,
    interface = config.get[String]("http.host"),
    port = config.get[Int]("http.port")
  )
}

