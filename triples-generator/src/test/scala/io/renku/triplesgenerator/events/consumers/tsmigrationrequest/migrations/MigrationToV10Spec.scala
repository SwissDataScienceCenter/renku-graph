/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.projectPaths
import cats.MonadThrow
import io.renku.generators.Generators.{exceptions, positiveInts}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling.{RecordsFinder, RecoverableErrorsRecovery}

class MigrationToV10Spec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "query" should {

    "find all projects that exists in the Default Graph but not in the Named Graphs dataset" in new TestCase {

      val allProjects = projectPaths.generateList(min = chunkSize, max = chunkSize * 2)

      val projectsChunks = allProjects
        .sliding(chunkSize, chunkSize)
        .toList
      projectsChunks.zipWithIndex foreach givenProjectsChunkReturned
      givenProjectsChunkReturned(List.empty[projects.Path] -> projectsChunks.size)

      allProjects map toCleanUpRequestEvent foreach givenEventIsSent

      migration.run().value.unsafeRunSync() shouldBe ().asRight
    }

    "return a Recoverable Error if in case of an exception while finding projects " +
      "the given strategy returns one" in new TestCase {

        val exception = exceptions.generateOne

        (projectsFinder.findProjects _)
          .expects(1)
          .returning(exception.raiseError[IO, List[projects.Path]])

        migration.run().value.unsafeRunSync() shouldBe recoverableError.asLeft
      }
  }

  private trait TestCase {
    val chunkSize = positiveInts(max = 100).generateOne.value

    private val eventCategoryName = CategoryName("CLEAN_UP_REQUEST")
    val projectsFinder            = mock[ProjectsFinder[IO]]
    private val eventSender       = mock[EventSender[IO]]
    val recoverableError          = processingRecoverableErrors.generateOne
    private val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new MigrationToV10[IO](projectsFinder, eventSender, recoveryStrategy)

    def givenProjectsChunkReturned: ((List[projects.Path], Int)) => Unit = { case (chunk, idx) =>
      (projectsFinder.findProjects _)
        .expects(idx + 1)
        .returning(chunk.pure[IO])
      ()
    }

    def toCleanUpRequestEvent(projectPath: projects.Path) = projectPath -> EventRequestContent.NoPayload {
      json"""{
        "categoryName": $eventCategoryName,
        "project": {
          "path": $projectPath
        }
      }"""
    }

    lazy val givenEventIsSent: ((projects.Path, EventRequestContent.NoPayload)) => Unit = { case (path, event) =>
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(event,
                 EventSender.EventContext(eventCategoryName,
                                          show"$categoryName: ${migration.name} cannot send event for $path"
                 )
        )
        .returning(().pure[IO])
      ()
    }
  }
}

class ProjectsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  private val chunkSize = 50

  "findProjects" should {

    "find requested page of projects existing in the TS" in new TestCase {

      val projects = anyProjectEntities
        .generateList(min = chunkSize + 1, max = Gen.choose(chunkSize + 1, (2 * chunkSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = projectsDataset, projects: _*)

      val (page1, page2) = projects splitAt chunkSize
      finder.findProjects(page = 1).unsafeRunSync() shouldBe page1.map(_.path)
      finder.findProjects(page = 2).unsafeRunSync() shouldBe page2.map(_.path)
      finder.findProjects(page = 3).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new ProjectsFinderImpl[IO](RecordsFinder(projectsDSConnectionInfo))
  }
}
