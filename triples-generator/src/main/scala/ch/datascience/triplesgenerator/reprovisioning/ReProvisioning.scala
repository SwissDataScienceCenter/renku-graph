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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.Timer
import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrlLoader
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import com.typesafe.config.{Config, ConfigFactory}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.util.control.NonFatal

trait ReProvisioning[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class ReProvisioningImpl[Interpretation[_]: MonadThrow: Timer](
    renkuVersionPairFinder:    RenkuVersionPairFinder[Interpretation],
    versionCompatibilityPairs: NonEmptyList[RenkuVersionPair],
    reprovisionJudge:          ReProvisionJudge,
    triplesRemover:            TriplesRemover[Interpretation],
    eventsReScheduler:         EventsReScheduler[Interpretation],
    renkuVersionPairUpdater:   RenkuVersionPairUpdater[Interpretation],
    reProvisioningStatus:      ReProvisioningStatus[Interpretation],
    executionTimeRecorder:     ExecutionTimeRecorder[Interpretation],
    logger:                    Logger[Interpretation],
    sleepWhenBusy:             FiniteDuration
) extends ReProvisioning[Interpretation] {

  import eventsReScheduler._
  import executionTimeRecorder._
  import reprovisionJudge.isReProvisioningNeeded
  import triplesRemover._

  override def run(): Interpretation[Unit] = for {
    maybeVersionPairInKG <- renkuVersionPairFinder.find() recoverWith tryAgain(renkuVersionPairFinder.find())
    _                    <- decideIfReProvisioningRequired(maybeVersionPairInKG)
  } yield ()

  private def decideIfReProvisioningRequired(maybeVersionPairInKG: Option[RenkuVersionPair]) =
    if (isReProvisioningNeeded(maybeVersionPairInKG, versionCompatibilityPairs))
      triggerReProvisioning recoverWith tryAgain(triggerReProvisioning)
    else
      renkuVersionPairUpdater.update(versionCompatibilityPairs.head) >> logger.info("All projects' triples up to date")

  private def triggerReProvisioning = measureExecutionTime {
    for {
      _ <- logger.info("The triples are not up to date - re-provisioning is clearing DB")
      _ <- reProvisioningStatus.setRunning() recoverWith tryAgain(reProvisioningStatus.setRunning())
      _ <- renkuVersionPairUpdater
             .update(versionCompatibilityPairs.head)
             .recoverWith(tryAgain(renkuVersionPairUpdater.update(versionCompatibilityPairs.head)))
      _ <- removeAllTriples() recoverWith tryAgain(removeAllTriples())
      _ <- triggerEventsReScheduling() recoverWith tryAgain(triggerEventsReScheduling())
      _ <- reProvisioningStatus.clear() recoverWith tryAgain(reProvisioningStatus.clear())
    } yield ()
  } flatMap logSummary

  private def logSummary: ((ElapsedTime, Unit)) => Interpretation[Unit] = { case (elapsedTime, _) =>
    logger.info(s"Clearing DB finished in ${elapsedTime}ms - re-processing all the events")
  }

  private def tryAgain[T](step: => Interpretation[T]): PartialFunction[Throwable, Interpretation[T]] = {
    case NonFatal(exception) =>
      {
        for {
          _      <- logger.error(exception)("Re-provisioning failure")
          _      <- Timer[Interpretation] sleep sleepWhenBusy
          result <- step
        } yield result
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
      reProvisioningStatus:      ReProvisioningStatus[IO],
      versionCompatibilityPairs: NonEmptyList[RenkuVersionPair],
      timeRecorder:              SparqlQueryTimeRecorder[IO],
      logger:                    Logger[IO],
      configuration:             Config = ConfigFactory.load()
  )(implicit
      ME:               MonadError[IO, Throwable],
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ReProvisioning[IO]] = for {
    rdfStoreConfig         <- RdfStoreConfig[IO](configuration)
    eventsReScheduler      <- IOEventsReScheduler(logger)
    renkuBaseUrl           <- RenkuBaseUrlLoader[IO]()
    executionTimeRecorder  <- ExecutionTimeRecorder[IO](ApplicationLogger)
    triplesRemover         <- TriplesRemoverImpl(rdfStoreConfig, logger, timeRecorder)
    renkuVersionPairFinder <- RenkuVersionPairFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  } yield new ReProvisioningImpl[IO](
    renkuVersionPairFinder,
    versionCompatibilityPairs,
    new ReProvisionJudgeImpl(),
    triplesRemover,
    eventsReScheduler,
    new RenkuVersionPairUpdaterImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
    reProvisioningStatus,
    executionTimeRecorder,
    logger,
    SleepWhenBusy
  )
}
