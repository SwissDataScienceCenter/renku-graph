package io.renku.http.server

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s._
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import scala.concurrent.duration._

object SimpleServer extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    server.useForever.as(ExitCode.Success)

  val routes: HttpRoutes[IO] =
    HttpRoutes.of { case GET -> Root =>
      IO.sleep(50.seconds).flatMap(_ => Ok())
    }

  val server =
    EmberServerBuilder
      .default[IO]
      .withPort(port"8088")
      .withHost(host"0.0.0.0")
      .withHttpApp(routes.orNotFound)
      .build
}
