/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

final case class ServiceRun(name:             String,
                            service:          IOMicroservice,
                            serviceClient:    ServiceClient,
                            preServiceStart:  List[IO[Unit]] = List.empty,
                            postServiceStart: List[IO[Unit]] = List.empty,
                            serviceArgsList:  List[() => String] = List.empty
)

class ServicesRunner(semaphore: Semaphore[IO]) {

  import ServiceClient.ServiceReadiness._
  import cats.syntax.all._

  import scala.concurrent.duration._

  private val logger = TestLogger()

  def run(services: ServiceRun*)(implicit ioRuntime: IORuntime): IO[Unit] =
    for {
      _ <- semaphore.acquire
      _ <- services.toList.map(start).parSequence
      _ <- semaphore.release
    } yield ()

  private val cancelTokens = new ConcurrentHashMap[ServiceRun, IO[Unit]]()

  private def start(serviceRun: ServiceRun)(implicit ioRuntime: IORuntime): IO[Unit] = {
    import serviceRun._
    serviceClient.ping.flatMap {
      case ServiceUp => IO.unit
      case _ =>
        for {
          _ <- logger.info(s"Service ${serviceRun.name} starting")
          _ <- preServiceStart.sequence
          _ = service
                .run(serviceRun.serviceArgsList.map(_()))
                .start
                .map(fiber => cancelTokens.put(serviceRun, fiber.cancel))
                .unsafeRunAndForget()
          _ <- verifyServiceReady(serviceRun)
        } yield ()
    }
  }

  private def verifyServiceReady(serviceRun: ServiceRun): IO[Unit] =
    serviceRun.serviceClient.ping flatMap {
      case ServiceUp =>
        serviceRun.postServiceStart.sequence flatMap (_ => logger.info(s"Service ${serviceRun.name} started"))
      case _ =>
        Temporal[IO].delayBy(verifyServiceReady(serviceRun), 500 millis)
    }

  private def verifyServiceDown(serviceRun: ServiceRun): IO[Unit] =
    serviceRun.serviceClient.ping flatMap {
      case ServiceUp => Temporal[IO].delayBy(verifyServiceReady(serviceRun), 500 millis)
      case _         => logger.info(s"Service ${serviceRun.name} stopped")
    }

  def restart(service: ServiceRun)(implicit ioRuntime: IORuntime): Unit = cancelTokens.asScala.get(service) match {
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

  def stop(serviceName: String)(implicit ioRuntime: IORuntime): Unit =
    cancelTokens.asScala.find { case (key, _) => key.name == serviceName } match {
      case None => throw new IllegalStateException(s"'$serviceName' service not found so cannot be restarted")
      case Some((_, cancelToken)) =>
        logger.info(s"$serviceName service stopping")
        cancelToken.unsafeRunSync()
    }

  def stopAllServices()(implicit ioRuntime: IORuntime): Unit = cancelTokens.asScala.foreach {
    case (service, cancelToken) =>
      logger.info(s"Service ${service.name} stopping")
      cancelToken.unsafeRunSync()
  }
}
