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

import ReProvisioningGenerators._
import cats.MonadError
import cats.data.OptionT
import cats.effect.{IO, Timer}
import cats.implicits._
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.dbeventlog.commands.{IOEventLogFetch, IOEventLogMarkAllNew}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class ReProvisionerSpec extends WordSpec with MockFactory {

  "run" should {

    s"recursively find all events having outdated version in the RDF Store and mark them in the Log with the $New status" in new TestCase {
      val project1OutdatedTriples = outdatedTriplesSets.generateOne
      val project2OutdatedTriples = outdatedTriplesSets.generateOne

      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(project1OutdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(project1OutdatedTriples.projectPath.toProjectPath, project1OutdatedTriples.commits.toCommitIds)
          .returning(context.unit)
        (triplesRemover
          .removeOutdatedTriples(_: OutdatedTriples))
          .expects(project1OutdatedTriples)
          .returning(context.unit)

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(project2OutdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(project2OutdatedTriples.projectPath.toProjectPath, project2OutdatedTriples.commits.toCommitIds)
          .returning(context.unit)
        (triplesRemover
          .removeOutdatedTriples(_: OutdatedTriples))
          .expects(project2OutdatedTriples)
          .returning(context.unit)

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      reProvisioner.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info(s"ReProvisioning '${project1OutdatedTriples.projectPath}' project"),
        Info(s"ReProvisioning '${project2OutdatedTriples.projectPath}' project"),
        Info("All projects' triples up to date")
      )
    }

    "wait with re-provisioning next project until there are no events to process" in new TestCase {
      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        val outdatedTriples = outdatedTriplesSets.generateOne
        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(outdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(outdatedTriples.projectPath.toProjectPath, outdatedTriples.commits.toCommitIds)
          .returning(context.unit)
        (triplesRemover
          .removeOutdatedTriples(_: OutdatedTriples))
          .expects(outdatedTriples)
          .returning(context.unit)

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(true))

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      reProvisioner.run.unsafeRunSync() shouldBe ((): Unit)
    }

    "do not fail but simply retry if checking if there are no events to process fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.raiseError(exception))

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      reProvisioner.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info("All projects' triples up to date")
      )
    }

    "do not fail but simply retry if finding outdated triples fails" in new TestCase {
      val exception = exceptions.generateOne

      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.raiseError(exception)))

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      reProvisioner.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info("All projects' triples up to date")
      )
    }

    "do not fail but simply retry if marking events to replay fails" in new TestCase {
      val exception       = exceptions.generateOne
      val outdatedTriples = outdatedTriplesSets.generateOne

      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(outdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(outdatedTriples.projectPath.toProjectPath, outdatedTriples.commits.toCommitIds)
          .returning(context.raiseError(exception))

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(outdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(outdatedTriples.projectPath.toProjectPath, outdatedTriples.commits.toCommitIds)
          .returning(context.unit)
        (triplesRemover
          .removeOutdatedTriples(_: OutdatedTriples))
          .expects(outdatedTriples)
          .returning(context.unit)

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      reProvisioner.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info(s"ReProvisioning '${outdatedTriples.projectPath}' project"),
        Info("All projects' triples up to date")
      )
    }

    "do not fail but simply retry if removing outdated triples fails" in new TestCase {
      val outdatedTriples = outdatedTriplesSets.generateOne
      val exception       = exceptions.generateOne

      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(outdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(outdatedTriples.projectPath.toProjectPath, outdatedTriples.commits.toCommitIds)
          .returning(context.unit)
        (triplesRemover
          .removeOutdatedTriples(_: OutdatedTriples))
          .expects(outdatedTriples)
          .returning(context.raiseError(exception))

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.liftF(context.pure(outdatedTriples)))
        (eventLogMarkNew
          .markEventsAsNew(_: ProjectPath, _: Set[CommitId]))
          .expects(outdatedTriples.projectPath.toProjectPath, outdatedTriples.commits.toCommitIds)
          .returning(context.unit)
        (triplesRemover
          .removeOutdatedTriples(_: OutdatedTriples))
          .expects(outdatedTriples)
          .returning(context.unit)

        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      reProvisioner.run.unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Error("Re-provisioning failure", exception),
        Info(s"ReProvisioning '${outdatedTriples.projectPath}' project"),
        Info("All projects' triples up to date")
      )
    }

    "start re-provisioning after the initial delay" in new TestCase {
      inSequence {
        (eventLogFetch.isEventToProcess _)
          .expects()
          .returning(context.pure(false))

        (triplesFinder.findOutdatedTriples _)
          .expects()
          .returning(OptionT.none[IO, OutdatedTriples])
      }

      val someInitialDelay: ReProvisioningDelay = ReProvisioningDelay(500 millis)

      val startTime = System.currentTimeMillis()

      new ReProvisioner[IO](
        triplesFinder,
        triplesRemover,
        eventLogMarkNew,
        eventLogFetch,
        someInitialDelay,
        logger,
        5 millis
      ).run.unsafeRunSync() shouldBe ((): Unit)

      val endTime = System.currentTimeMillis()

      (endTime - startTime) should be > someInitialDelay.value.toMillis
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val triplesFinder   = mock[OutdatedTriplesFinder[IO]]
    val triplesRemover  = mock[OutdatedTriplesRemover[IO]]
    val eventLogMarkNew = mock[IOEventLogMarkAllNew]
    val eventLogFetch   = mock[IOEventLogFetch]
    val initialDelay    = ReProvisioningDelay(durations(100 millis).generateOne)
    val logger          = TestLogger[IO]()
    val reProvisioner = new ReProvisioner[IO](
      triplesFinder,
      triplesRemover,
      eventLogMarkNew,
      eventLogFetch,
      initialDelay,
      logger,
      5 millis
    )
  }

  private implicit class CommitIdResourceOps(commitIdResources: Set[CommitIdResource]) {
    lazy val toCommitIds = commitIdResources.map(_.to[Try, CommitId].fold(throw _, identity))
  }

  private implicit class FullProjectPathOps(projectPath: FullProjectPath) {
    lazy val toProjectPath = projectPath.to[Try, ProjectPath].fold(throw _, identity)
  }
}
