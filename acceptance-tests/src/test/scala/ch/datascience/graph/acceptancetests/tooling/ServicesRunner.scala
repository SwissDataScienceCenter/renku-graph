/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.tooling

import java.util.concurrent.ConcurrentHashMap

import cats.effect._
import cats.effect.concurrent.Semaphore
import ch.datascience.logging.IOLogger
import ch.datascience.microservices.IOMicroservice
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext

final case class ServiceRun(name:             String,
                            service:          IOMicroservice,
                            serviceClient:    ServiceClient,
                            preServiceStart:  List[IO[Unit]] = List.empty,
                            postServiceStart: List[IO[Unit]] = List.empty
)

class ServicesRunner(
    semaphore:               Semaphore[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]) {

  import ServiceClient.ServiceReadiness._
  import cats.syntax.all._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  private val logger = new IOLogger(LoggerFactory.getLogger("test"))

  def run(services: ServiceRun*): IO[Unit] =
    for {
      _ <- semaphore.acquire
      _ <- services.toList.map(start).parSequence
      _ <- semaphore.release
    } yield ()

  private val cancelTokens = new ConcurrentHashMap[ServiceRun, CancelToken[IO]]()

  private def start(
      serviceRun:              ServiceRun
  )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]): IO[Unit] = {
    import serviceRun._
    serviceClient.ping.flatMap {
      case ServiceUp => IO.unit
      case _ =>
        for {
          _ <- logger.info(s"Service ${serviceRun.name} starting")
          _ <- preServiceStart.sequence
          _ = service.run(Nil).start.map(fiber => cancelTokens.put(serviceRun, fiber.cancel)).unsafeRunAsyncAndForget()
          _ <- verifyServiceReady(serviceRun)
        } yield ()
    }
  }

  private def verifyServiceReady(serviceRun: ServiceRun)(implicit timer: Timer[IO]): IO[Unit] =
    serviceRun.serviceClient.ping flatMap {
      case ServiceUp =>
        serviceRun.postServiceStart.sequence flatMap (_ => logger.info(s"Service ${serviceRun.name} started"))
      case _ =>
        (timer sleep (500 millis)) flatMap (_ => verifyServiceReady(serviceRun))
    }

  private def verifyServiceDown(serviceRun: ServiceRun)(implicit timer: Timer[IO]): IO[Unit] =
    serviceRun.serviceClient.ping flatMap {
      case ServiceUp => (timer sleep (500 millis)) flatMap (_ => verifyServiceDown(serviceRun))
      case _         => logger.info(s"Service ${serviceRun.name} stopped")
    }

  def restart(service: ServiceRun): Unit = cancelTokens.asScala.get(service) match {
    case None => throw new IllegalStateException(s"'${service.name}' service not found so cannot be restarted")
    case Some(cancelToken) =>
      {
        for {
          _ <- logger.info(s"Service ${service.name} stopping")
          _ <- service.service.stopSubProcesses.sequence
          _ <- cancelToken
          _ <- verifyServiceDown(service)
          _ = cancelTokens.remove(service)
          _ <- start(service)
        } yield ()
      }.unsafeRunSync()
  }

  def stopAllServices(): Unit = cancelTokens.asScala.foreach { case (service, cancelToken) =>
    logger.info(s"Service ${service.name} stopping")
    cancelToken.unsafeRunSync()
  }
}
