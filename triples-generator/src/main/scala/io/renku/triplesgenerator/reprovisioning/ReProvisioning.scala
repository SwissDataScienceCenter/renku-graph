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

package io.renku.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.RenkuVersionPair
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.microservices.{MicroserviceIdentifier, MicroserviceUrlFinder}
import io.renku.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.Microservice
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.util.control.NonFatal

trait ReProvisioning[F[_]] {
  def run(): F[Unit]
}

class ReProvisioningImpl[F[_]: Temporal: Logger](
    versionCompatibilityPairs: NonEmptyList[RenkuVersionPair],
    reProvisionJudge:          ReProvisionJudge[F],
    triplesRemover:            TriplesRemover[F],
    eventsReScheduler:         EventsReScheduler[F],
    versionPairUpdater:        RenkuVersionPairUpdater[F],
    microserviceUrlFinder:     MicroserviceUrlFinder[F],
    reProvisioningStatus:      ReProvisioningStatus[F],
    executionTimeRecorder:     ExecutionTimeRecorder[F],
    microserviceIdentifier:    MicroserviceIdentifier,
    sleepWhenBusy:             FiniteDuration
) extends ReProvisioning[F] {

  import eventsReScheduler._
  import executionTimeRecorder._
  import reProvisionJudge.reProvisioningNeeded
  import triplesRemover._

  override def run(): F[Unit] = (reProvisioningNeeded() recoverWith tryAgain(reProvisioningNeeded())) >>= {
    case true =>
      triggerReProvisioning recoverWith tryAgain(triggerReProvisioning)
    case false =>
      versionPairUpdater.update(versionCompatibilityPairs.head) >> Logger[F].info("Triples Store up to date")
  }

  private def triggerReProvisioning = measureExecutionTime {
    for {
      _ <- Logger[F].info("Triples Store is not on the required schema version - kicking-off re-provisioning")
      _ <- setRunningStatusInTS()
      _ <- versionPairUpdater
             .update(versionCompatibilityPairs.head)
             .recoverWith(tryAgain(versionPairUpdater.update(versionCompatibilityPairs.head)))
      _ <- removeAllTriples() recoverWith tryAgain(removeAllTriples())
      _ <- triggerEventsReScheduling() recoverWith tryAgain(triggerEventsReScheduling())
      _ <- reProvisioningStatus.clear() recoverWith tryAgain(reProvisioningStatus.clear())
    } yield ()
  } >>= logSummary

  private def setRunningStatusInTS() = findControllerInfo >>= { controllerInfo =>
    reProvisioningStatus.setRunning(on = controllerInfo) recoverWith tryAgain(
      reProvisioningStatus.setRunning(controllerInfo)
    )
  }

  private lazy val findControllerInfo = microserviceUrlFinder
    .findBaseUrl()
    .recoverWith(tryAgain(microserviceUrlFinder.findBaseUrl()))
    .map(Controller(_, microserviceIdentifier))

  private def logSummary: ((ElapsedTime, Unit)) => F[Unit] = { case (elapsedTime, _) =>
    Logger[F].info(s"Clearing DB finished in ${elapsedTime}ms - re-processing all the events")
  }

  private def tryAgain[T](step: => F[T]): PartialFunction[Throwable, F[T]] = { case NonFatal(exception) =>
    Logger[F].error(exception)("Re-provisioning failure") >>
      Temporal[F].delayBy(step, sleepWhenBusy) recoverWith tryAgain(step)
  }
}

object ReProvisioning {

  import scala.concurrent.duration._

  private val SleepWhenBusy = 10 minutes

  def apply[F[_]: Async: Logger](
      reProvisioningStatus: ReProvisioningStatus[F],
      compatibilityMatrix:  NonEmptyList[RenkuVersionPair],
      timeRecorder:         SparqlQueryTimeRecorder[F],
      configuration:        Config = ConfigFactory.load()
  ): F[ReProvisioning[F]] = RenkuBaseUrlLoader[F]() flatMap { implicit renkuBaseUrl =>
    for {
      rdfStoreConfig        <- RdfStoreConfig[F](configuration)
      eventsReScheduler     <- EventsReScheduler[F]
      microserviceUrlFinder <- MicroserviceUrlFinder[F](Microservice.ServicePort)
      executionTimeRecorder <- ExecutionTimeRecorder[F]()
      triplesRemover        <- TriplesRemoverImpl(rdfStoreConfig, timeRecorder)
      reProvisioningJudge <- ReProvisionJudge[F](rdfStoreConfig,
                                                 reProvisioningStatus,
                                                 microserviceUrlFinder,
                                                 compatibilityMatrix,
                                                 timeRecorder
                             )
    } yield new ReProvisioningImpl[F](
      compatibilityMatrix,
      reProvisioningJudge,
      triplesRemover,
      eventsReScheduler,
      new RenkuVersionPairUpdaterImpl(rdfStoreConfig, timeRecorder),
      microserviceUrlFinder,
      reProvisioningStatus,
      executionTimeRecorder,
      Microservice.Identifier,
      SleepWhenBusy
    )
  }
}
