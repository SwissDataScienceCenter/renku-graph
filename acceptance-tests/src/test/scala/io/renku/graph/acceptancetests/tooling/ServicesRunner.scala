/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.acceptancetests.tooling

import cats.effect._
import cats.effect.std.Semaphore
import cats.effect.unsafe.IORuntime
import io.renku.microservices.IOMicroservice
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

final case class ServiceRun(name:             String,
                            service:          IOMicroservice,
                            serviceClient:    ServiceClient,
                            preServiceStart:  List[IO[Unit]] = List.empty,
                            postServiceStart: List[IO[Unit]] = List.empty,
                            onServiceStop:    List[IO[Unit]] = List.empty,
                            serviceArgsList:  List[() => String] = List.empty
)

object ServicesState {
  val cancelTokens = new ConcurrentHashMap[ServiceRun, IO[Unit]]()
}

class ServicesRunner(semaphore: Semaphore[IO])(implicit
    logger:                     Logger[IO]
) {

  import ServiceClient.ServiceReadiness._
  import cats.syntax.all._

  import scala.concurrent.duration._

  def run(services: ServiceRun*)(implicit ioRuntime: IORuntime): IO[Unit] = for {
    _ <- semaphore.acquire
    _ <- services.toList.map(start).parSequence
    _ <- semaphore.release
  } yield ()

  private def start(serviceRun: ServiceRun)(implicit ioRuntime: IORuntime): IO[Unit] = {
    import serviceRun._
    serviceClient.ping >>= {
      case ServiceUp => IO.unit
      case _ =>
        for {
          _ <- logger.info(s"Service ${serviceRun.name} starting")
          _ <- preServiceStart.sequence
          _ <- service
                 .run(serviceRun.serviceArgsList.map(_()))
                 .start
                 .map(fiber => ServicesState.cancelTokens.put(serviceRun, fiber.cancel))
          _ <- verifyServiceReady(serviceRun)
        } yield ()
    }
  }

  private def verifyServiceReady(serviceRun: ServiceRun): IO[Unit] =
    serviceRun.serviceClient.ping >>= {
      case ServiceUp => serviceRun.postServiceStart.sequence >> logger.info(s"Service ${serviceRun.name} started")
      case _         => Temporal[IO].delayBy(verifyServiceReady(serviceRun), 500 millis)
    }

  def stop(service: ServiceRun)(implicit ioRuntime: IORuntime): Unit =
    ServicesState.cancelTokens.asScala.find { case (key, _) => key.name == service.name } match {
      case None => throw new IllegalStateException(s"'${service.name}' service not found, it is already stopped")
      case Some((service, cancelToken)) =>
        { service.onServiceStop.sequence >> cancelToken >> verifyServiceStopped(service) }.unsafeRunSync()
    }

  private def verifyServiceStopped(serviceRun: ServiceRun): IO[Unit] =
    serviceRun.serviceClient.ping >>= {
      case ServiceDown => logger.info(s"Service ${serviceRun.name} stopped")
      case _ =>
        logger.info(s"Kill signal sent to ${serviceRun.name} but it's still running...") >>
          Temporal[IO].delayBy(verifyServiceStopped(serviceRun), 500 millis)
    }
}
