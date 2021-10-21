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

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.Timer
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.RenkuVersionPair
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.renku.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.util.control.NonFatal

trait ReProvisioning[F[_]] {
  def run(): F[Unit]
}

class ReProvisioningImpl[F[_]: MonadThrow: Timer](
    renkuVersionPairFinder:    RenkuVersionPairFinder[F],
    versionCompatibilityPairs: NonEmptyList[RenkuVersionPair],
    reprovisionJudge:          ReProvisionJudge,
    triplesRemover:            TriplesRemover[F],
    eventsReScheduler:         EventsReScheduler[F],
    renkuVersionPairUpdater:   RenkuVersionPairUpdater[F],
    reProvisioningStatus:      ReProvisioningStatus[F],
    executionTimeRecorder:     ExecutionTimeRecorder[F],
    logger:                    Logger[F],
    sleepWhenBusy:             FiniteDuration
) extends ReProvisioning[F] {

  import eventsReScheduler._
  import executionTimeRecorder._
  import reprovisionJudge.isReProvisioningNeeded
  import triplesRemover._

  override def run(): F[Unit] = for {
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

  private def logSummary: ((ElapsedTime, Unit)) => F[Unit] = { case (elapsedTime, _) =>
    logger.info(s"Clearing DB finished in ${elapsedTime}ms - re-processing all the events")
  }

  private def tryAgain[T](step: => F[T]): PartialFunction[Throwable, F[T]] = { case NonFatal(exception) =>
    {
      for {
        _      <- logger.error(exception)("Re-provisioning failure")
        _      <- Timer[F] sleep sleepWhenBusy
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
