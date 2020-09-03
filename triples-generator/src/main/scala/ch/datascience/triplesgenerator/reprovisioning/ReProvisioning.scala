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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.effect.Timer
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import com.typesafe.config.{Config, ConfigFactory}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

trait ReProvisioning[Interpretation[_]] {
  def run: Interpretation[Unit]
}

class ReProvisioningImpl[Interpretation[_]](
    triplesVersionFinder:     TriplesVersionFinder[Interpretation],
    triplesRemover:           TriplesRemover[Interpretation],
    eventsReScheduler:        EventsReScheduler[Interpretation],
    reProvisioningFlagSetter: ReProvisioningFlagSetter[Interpretation],
    triplesVersionCreator:    TriplesVersionCreator[Interpretation],
    subscriber:               Subscriber[Interpretation],
    reProvisioningDelay:      ReProvisioningDelay,
    executionTimeRecorder:    ExecutionTimeRecorder[Interpretation],
    logger:                   Logger[Interpretation],
    sleepWhenBusy:            FiniteDuration
)(implicit ME:                MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends ReProvisioning[Interpretation] {

  import eventsReScheduler._
  import executionTimeRecorder._
  import reProvisioningFlagSetter._
  import subscriber.notifyAvailability
  import triplesRemover._
  import triplesVersionCreator._
  import triplesVersionFinder._

  override def run: Interpretation[Unit] =
    for {
      _ <- timer sleep reProvisioningDelay.value
      _ <- startReProvisioning
    } yield ()

  private def startReProvisioning: Interpretation[Unit] =
    triplesUpToDate.flatMap {
      case false => triggerReProvisioning
      case true  => logger.info("All projects' triples up to date")
    } recoverWith tryAgain(startReProvisioning)

  private def triggerReProvisioning =
    measureExecutionTime {
      for {
        _ <- logger.info("The triples are not up to date - clearing DB and re-scheduling all the events")
        _ <- setUnderReProvisioningFlag()
        _ <- updateCliVersion()
        _ <- removeAllTriples() recoverWith tryAgain(removeAllTriples())
        _ <- triggerEventsReScheduling recoverWith tryAgain(triggerEventsReScheduling)
        _ <- clearUnderReProvisioningFlag recoverWith tryAgain(clearUnderReProvisioningFlag)
        _ <- notifyAvailability recoverWith tryAgain(notifyAvailability)
      } yield ()
    } flatMap logSummary

  private def logSummary: ((ElapsedTime, Unit)) => Interpretation[Unit] = {
    case (elapsedTime, _) => logger.info(s"ReProvisioning triggered in ${elapsedTime}ms")
  }

  private def tryAgain(step: => Interpretation[Unit]): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) => {
      for {
        _ <- logger.error(exception)("Re-provisioning failure")
        _ <- timer sleep sleepWhenBusy
        _ <- step
      } yield ()
    } recoverWith tryAgain(step)
  }
}

class ReProvisioningDelay private (val value: FiniteDuration) extends TinyType {
  type V = FiniteDuration
}
object ReProvisioningDelay extends TinyTypeFactory[ReProvisioningDelay](new ReProvisioningDelay(_))

object IOReProvisioning {

  import cats.MonadError
  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.ConfigLoader._

  import scala.concurrent.ExecutionContext
  import scala.concurrent.duration._

  private val SleepWhenBusy = 10 minutes

  def apply(
      triplesGeneration: TriplesGeneration,
      subscriber:        Subscriber[IO],
      timeRecorder:      SparqlQueryTimeRecorder[IO],
      logger:            Logger[IO],
      configuration:     Config = ConfigFactory.load()
  )(implicit ME:         MonadError[IO, Throwable],
    executionContext:    ExecutionContext,
    contextShift:        ContextShift[IO],
    timer:               Timer[IO]): IO[ReProvisioning[IO]] =
    for {
      rdfStoreConfig    <- RdfStoreConfig[IO](configuration)
      currentCliVersion <- CliVersionFinder[IO](triplesGeneration)
      eventsReScheduler <- IOEventsReScheduler(logger)
      renkuBaseUrl      <- RenkuBaseUrl[IO]()
      initialDelay <- find[IO, FiniteDuration]("re-provisioning-initial-delay", configuration)
                       .flatMap(delay => ME.fromEither(ReProvisioningDelay.from(delay)))
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
      triplesRemover        <- IOTriplesRemover(rdfStoreConfig, logger, timeRecorder)
    } yield new ReProvisioningImpl[IO](
      new IOTriplesVersionFinder(rdfStoreConfig, currentCliVersion, logger, timeRecorder),
      triplesRemover,
      eventsReScheduler,
      new ReProvisioningFlagSetterImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      new IOTriplesVersionCreator(rdfStoreConfig, currentCliVersion, renkuBaseUrl, logger, timeRecorder),
      subscriber,
      initialDelay,
      executionTimeRecorder,
      logger,
      SleepWhenBusy
    )
}
