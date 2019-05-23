/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.effect.concurrent.Semaphore

import scala.concurrent.ExecutionContext

final case class ServiceRun(service:          IOApp,
                            serviceClient:    ServiceClient,
                            preServiceStart:  List[IO[Unit]] = List.empty,
                            postServiceStart: List[IO[Unit]] = List.empty)

class ServicesRunner(
    semaphore:               Semaphore[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]) {

  import ServiceClient.ServiceReadiness._
  import cats.implicits._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  def run(services: ServiceRun*): IO[Unit] =
    for {
      _ <- semaphore.acquire
      _ <- services.toList.map(start).parSequence
      _ <- semaphore.release
    } yield ()

  private def start(
      serviceRun:              ServiceRun
  )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]): IO[Unit] = {
    import serviceRun._
    serviceClient.ping.flatMap {
      case ServiceUp => IO.unit
      case _ =>
        for {
          _ <- preServiceStart.sequence
          _ = IO(service.main(Array.empty)).start.unsafeRunAsyncAndForget()
          _ <- verifyServiceReady(serviceRun)
        } yield ()
    }
  }

  private def verifyServiceReady(serviceRun: ServiceRun)(implicit timer: Timer[IO]): IO[Unit] =
    serviceRun.serviceClient.ping flatMap {
      case ServiceUp => serviceRun.postServiceStart.sequence map (_ => ())
      case _         => timer.sleep(500 millis) *> verifyServiceReady(serviceRun)
    }
}
