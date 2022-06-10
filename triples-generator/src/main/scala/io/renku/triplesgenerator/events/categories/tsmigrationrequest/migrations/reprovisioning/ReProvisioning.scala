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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations
package reprovisioning

import cats.data.{EitherT, NonEmptyList}
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuVersionPair
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.MicroserviceUrlFinder
import io.renku.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.Microservice
import io.renku.triplesgenerator.config.VersionCompatibilityConfig
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.ConditionedMigration.MigrationRequired
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

private class ReProvisioningImpl[F[_]: Temporal: Logger](
    versionCompatibilityPairs: NonEmptyList[RenkuVersionPair],
    reProvisionJudge:          ReProvisionJudge[F],
    triplesRemover:            TriplesRemover[F],
    eventSender:               EventSender[F],
    versionPairUpdater:        RenkuVersionPairUpdater[F],
    microserviceUrlFinder:     MicroserviceUrlFinder[F],
    reProvisioningStatus:      ReProvisioningStatus[F],
    executionTimeRecorder:     ExecutionTimeRecorder[F],
    retryDelay:                FiniteDuration
) extends ConditionedMigration[F] {

  override val name: Migration.Name = migrationName

  import eventSender._
  import executionTimeRecorder._
  import reProvisionJudge.reProvisioningNeeded
  import triplesRemover._

  override def required: EitherT[F, ProcessingRecoverableError, MigrationRequired] = EitherT.right {
    reProvisioningNeeded()
      .recoverWith(tryAgain(reProvisioningNeeded()))
      .map {
        case true  => MigrationRequired.Yes("TS in incompatible version")
        case false => MigrationRequired.No("TS up to date")
      }
  }

  override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT.right {
    triggerReProvisioning recoverWith tryAgain(triggerReProvisioning)
  }

  private def triggerReProvisioning = measureExecutionTime {
    for {
      _ <- setRunningStatusInTS()
      _ <- versionPairUpdater
             .update(versionCompatibilityPairs.head)
             .recoverWith(tryAgain(versionPairUpdater.update(versionCompatibilityPairs.head)))
      _ <- removeAllTriples() recoverWith tryAgain(removeAllTriples())
      _ <- sendStatusChangeEvent() recoverWith tryAgain(sendStatusChangeEvent())
    } yield ()
  } >>= logSummary

  override def postMigration(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT.right {
    reProvisioningStatus.clear() recoverWith tryAgain(reProvisioningStatus.clear())
  }

  private def setRunningStatusInTS() = findControllerUrl >>= { controllerUrl =>
    reProvisioningStatus.setRunning(on = controllerUrl) recoverWith tryAgain(
      reProvisioningStatus.setRunning(on = controllerUrl)
    )
  }

  private lazy val findControllerUrl = microserviceUrlFinder
    .findBaseUrl()
    .recoverWith(tryAgain(microserviceUrlFinder.findBaseUrl()))

  private def sendStatusChangeEvent() = sendEvent(
    EventRequestContent.NoPayload(json"""{"categoryName": "EVENTS_STATUS_CHANGE", "newStatus": "NEW"}"""),
    EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"), formMessage("sending EVENTS_STATUS_CHANGE failed"))
  )

  private def logSummary: ((ElapsedTime, Unit)) => F[Unit] = { case (elapsedTime, _) =>
    Logger[F].info(formMessage(show"TS cleared in ${elapsedTime}ms - re-processing all the events"))
  }

  private def tryAgain[T](step: => F[T]): PartialFunction[Throwable, F[T]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(formMessage("failure")) >>
      Temporal[F].delayBy(step, retryDelay) recoverWith tryAgain(step)
  }
}

private[migrations] object ReProvisioning {

  import io.renku.config.ConfigLoader.find

  import scala.concurrent.duration._

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      reProvisioningStatus: ReProvisioningStatus[F],
      config:               Config = ConfigFactory.load()
  ): F[Migration[F]] = RenkuUrlLoader[F]() flatMap { implicit renkuUrl =>
    for {
      retryDelay            <- find[F, FiniteDuration]("re-provisioning-retry-delay", config)
      rdfStoreConfig        <- RdfStoreConfig[F](config)
      eventSender           <- EventSender[F]
      microserviceUrlFinder <- MicroserviceUrlFinder[F](Microservice.ServicePort)
      compatibilityMatrix   <- VersionCompatibilityConfig[F](config)
      executionTimeRecorder <- ExecutionTimeRecorder[F]()
      triplesRemover        <- TriplesRemoverImpl(rdfStoreConfig)
      judge <- ReProvisionJudge[F](rdfStoreConfig, reProvisioningStatus, microserviceUrlFinder, compatibilityMatrix)
    } yield new ReProvisioningImpl[F](
      compatibilityMatrix,
      judge,
      triplesRemover,
      eventSender,
      new RenkuVersionPairUpdaterImpl(rdfStoreConfig),
      microserviceUrlFinder,
      reProvisioningStatus,
      executionTimeRecorder,
      retryDelay
    )
  }
}
