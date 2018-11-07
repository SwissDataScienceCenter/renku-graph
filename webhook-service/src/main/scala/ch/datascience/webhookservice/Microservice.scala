package ch.datascience.webhookservice

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.datascience.webhookservice.config.ConfigOps.Implicits._
import com.typesafe.config.ConfigFactory

object Microservice extends App {
  private implicit val system: ActorSystem = ActorSystem()
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val config = ConfigFactory.load()
  private val logger = Logging(system, getClass)
  private val webhookEndpoint = WebhookEndpoint(logger)

  Http().bindAndHandle(
    handler = webhookEndpoint.routes,
    interface = config.get[String]("http.host"),
    port = config.get[Int]("http.port")
  )
}

