package io.renku.http.server

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.comcast.ip4s._
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server

import scala.concurrent.duration._

object SimpleServer extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    server(port"8088").useForever.as(ExitCode.Success)

  val routes: HttpRoutes[IO] =
    HttpRoutes.of { case GET -> Root =>
      IO.sleep(50.seconds).flatMap(_ => Ok())
    }

  def server(port: Port): Resource[IO, Server] =
    EmberServerBuilder
      .default[IO]
      .withPort(port)
      .withHost(host"0.0.0.0")
      .withHttpApp(routes.orNotFound)
      .build
}
