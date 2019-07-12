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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.data.OptionT._
import cats.effect.Timer
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.{EventLogFetch, EventLogMarkAllNew}
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.RdfStoreConfig
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import ch.datascience.triplesgenerator.config.TriplesGeneration
import com.typesafe.config.{Config, ConfigFactory}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, postfixOps}
import scala.util.control.NonFatal

class ReProvisioner[Interpretation[_]](
    triplesFinder:       OutdatedTriplesFinder[Interpretation],
    triplesRemover:      OutdatedTriplesRemover[Interpretation],
    eventLogMarkAllNew:  EventLogMarkAllNew[Interpretation],
    eventLogFetch:       EventLogFetch[Interpretation],
    reProvisioningDelay: ReProvisioningDelay,
    logger:              Logger[Interpretation],
    sleepWhenBusy:       FiniteDuration
)(implicit ME:           MonadError[Interpretation, Throwable], timer: Timer[Interpretation]) {

  import eventLogFetch._
  import eventLogMarkAllNew._
  import triplesFinder._
  import triplesRemover._

  def run: Interpretation[Unit] =
    timer.sleep(reProvisioningDelay.value) *> startReProvisioning

  private def startReProvisioning: Interpretation[Unit] =
    isEventToProcess flatMap {
      case false => reProvisionNextProject
      case true  => (timer sleep sleepWhenBusy) *> startReProvisioning
    } recoverWith tryAgain

  private def reProvisionNextProject: Interpretation[Unit] = {
    for {
      outdatedTriples <- findOutdatedTriples
      projectPath     <- liftF(outdatedTriples.projectPath.to[Interpretation, ProjectPath])
      commitIds       <- liftF(outdatedTriples.commits.toList.map(_.to[Interpretation, CommitId]).sequence)
      _               <- liftF(markEventsAsNew(projectPath, commitIds.toSet))
      _               <- liftF(removeOutdatedTriples(outdatedTriples))
      _               <- liftF(logger.info(s"ReProvisioning '${outdatedTriples.projectPath}' project"))
      _               <- liftF(startReProvisioning)
    } yield ()
  }.value.flatMap(_ => logger.info("All projects' triples up to date"))

  private lazy val tryAgain: PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)("Re-provisioning failure")
        _ <- timer sleep sleepWhenBusy
        _ <- startReProvisioning
      } yield ()
  }
}

class ReProvisioningDelay private (val value: FiniteDuration) extends TinyType {
  type V = FiniteDuration
}
object ReProvisioningDelay extends TinyTypeFactory[ReProvisioningDelay](new ReProvisioningDelay(_))

object IOReProvisioner {

  import cats.MonadError
  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.config.ConfigLoader._

  import scala.concurrent.ExecutionContext
  import scala.concurrent.duration._

  private val SleepWhenBusy = 10 second

  def apply(
      triplesGeneration: TriplesGeneration,
      dbTransactor:      DbTransactor[IO, EventLogDB],
      configuration:     Config = ConfigFactory.load(),
      logger:            Logger[IO] = ApplicationLogger
  )(implicit ME:         MonadError[IO, Throwable],
    executionContext:    ExecutionContext,
    contextShift:        ContextShift[IO],
    timer:               Timer[IO]): IO[ReProvisioner[IO]] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO](configuration)
      schemaVersion  <- SchemaVersionFinder[IO](triplesGeneration)
      initialDelay <- find[IO, FiniteDuration]("re-provisioning-initial-delay", configuration).flatMap(delay =>
                       ME.fromEither(ReProvisioningDelay.from(delay)))
    } yield
      new ReProvisioner[IO](
        new IOOutdatedTriplesFinder(rdfStoreConfig, schemaVersion, logger),
        new IOOutdatedTriplesRemover(rdfStoreConfig, logger),
        new EventLogMarkAllNew[IO](dbTransactor),
        new EventLogFetch[IO](dbTransactor),
        initialDelay,
        logger,
        SleepWhenBusy
      )
}
