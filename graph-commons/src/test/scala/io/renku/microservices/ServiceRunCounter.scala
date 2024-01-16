/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.microservices

import cats.data.Kleisli
import cats.effect._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.EventRequestContent
import io.renku.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import io.renku.http.server.HttpServer
import io.renku.metrics.GaugeResetScheduler
import org.http4s.{HttpApp, Response}
import org.http4s.server.Server

import java.net.InetSocketAddress

class ServiceRunCounter(val name: String)
    extends ServiceReadinessChecker[IO]
    with CertificateLoader[IO]
    with SentryInitializer[IO]
    with EventConsumersRegistry[IO]
    with GaugeResetScheduler[IO]
    with HttpServer[IO]
    with CallCounter {

  def run: IO[Unit] = counter.updateAndGet(_.map(_ + 1)).rethrow.as(())

  override def waitIfNotUp: IO[Unit] = run

  override val httpApp: HttpApp[IO] = Kleisli(req => Response.notFoundFor(req))

  override def handle(requestContent: EventRequestContent): IO[EventSchedulingResult] = ???

  override def createServer: Resource[IO, Server] =
    Resource
      .eval(run)
      .map(_ =>
        new Server {
          override def address:  InetSocketAddress = InetSocketAddress.createUnresolved("localhost", 8080)
          override def isSecure: Boolean           = false
        }
      )

  override def renewAllSubscriptions(): IO[Unit] = ???
}
