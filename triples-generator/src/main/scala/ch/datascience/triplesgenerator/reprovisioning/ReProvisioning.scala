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
import ch.datascience.triplesgenerator.config.TriplesGeneration
import com.typesafe.config.{Config, ConfigFactory}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

trait ReProvisioning[Interpretation[_]] {
  def run: Interpretation[Unit]
}

class ReProvisioningImpl[Interpretation[_]](
    triplesVersionFinder:  TriplesVersionFinder[Interpretation],
    triplesRemover:        TriplesRemover[Interpretation],
    eventsReScheduler:     EventsReScheduler[Interpretation],
    triplesVersionCreator: TriplesVersionCreator[Interpretation],
    reProvisioningStatus:  ReProvisioningStatus[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation],
    sleepWhenBusy:         FiniteDuration
)(implicit ME:             MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends ReProvisioning[Interpretation] {

  import eventsReScheduler._
  import executionTimeRecorder._
  import triplesRemover._
  import triplesVersionCreator._
  import triplesVersionFinder._

  override def run: Interpretation[Unit] =
    triplesUpToDate.flatMap {
      case false => triggerReProvisioning
      case true  => logger.info("All projects' triples up to date")
    } recoverWith tryAgain(run)

  private def triggerReProvisioning =
    measureExecutionTime {
      for {
        _ <- logger.info("The triples are not up to date - clearing DB and re-scheduling all the events")
        _ <- reProvisioningStatus.setRunning()
        _ <- updateCliVersion()
        _ <- removeAllTriples() recoverWith tryAgain(removeAllTriples())
        _ <- triggerEventsReScheduling recoverWith tryAgain(triggerEventsReScheduling)
        _ <- reProvisioningStatus.clear() recoverWith tryAgain(reProvisioningStatus.clear())
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

object IOReProvisioning {

  import cats.MonadError
  import cats.effect.{ContextShift, IO, Timer}

  import scala.concurrent.ExecutionContext
  import scala.concurrent.duration._

  private val SleepWhenBusy = 10 minutes

  def apply(
      triplesGeneration:    TriplesGeneration,
      reProvisioningStatus: ReProvisioningStatus[IO],
      timeRecorder:         SparqlQueryTimeRecorder[IO],
      logger:               Logger[IO],
      configuration:        Config = ConfigFactory.load()
  )(implicit ME:            MonadError[IO, Throwable],
    executionContext:       ExecutionContext,
    contextShift:           ContextShift[IO],
    timer:                  Timer[IO]): IO[ReProvisioning[IO]] =
    for {
      rdfStoreConfig        <- RdfStoreConfig[IO](configuration)
      currentCliVersion     <- CliVersionFinder[IO](triplesGeneration)
      eventsReScheduler     <- IOEventsReScheduler(logger)
      renkuBaseUrl          <- RenkuBaseUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
      triplesRemover        <- IOTriplesRemover(rdfStoreConfig, logger, timeRecorder)
    } yield new ReProvisioningImpl[IO](
      new IOTriplesVersionFinder(rdfStoreConfig, currentCliVersion, logger, timeRecorder),
      triplesRemover,
      eventsReScheduler,
      new IOTriplesVersionCreator(rdfStoreConfig, currentCliVersion, renkuBaseUrl, logger, timeRecorder),
      reProvisioningStatus,
      executionTimeRecorder,
      logger,
      SleepWhenBusy
    )
}
