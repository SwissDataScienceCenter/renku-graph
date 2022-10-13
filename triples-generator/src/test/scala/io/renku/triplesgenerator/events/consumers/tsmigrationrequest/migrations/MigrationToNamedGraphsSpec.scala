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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import DefaultGraphProjectsFinder.ProjectInfo
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.{projectPaths, projectResourceIds}
import cats.MonadThrow
import io.renku.generators.Generators.{exceptions, positiveInts}
import io.renku.graph.model.events.EventStatus.TriplesGenerated
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

class MigrationToNamedGraphsSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "query" should {

    "find all projects that exists in the Default Graph but not in the Named Graphs dataset" in new TestCase {

      val defaultOnlyProjects = projectInfos.generateList(max = chunkSize.value)
      val bothDSProjects      = projectInfos.generateNonEmptyList().toList

      val allDefaultDSProjectsChunks = (defaultOnlyProjects ::: bothDSProjects)
        .sortBy(_._2)
        .sliding(chunkSize.value, chunkSize.value)
        .toList
      allDefaultDSProjectsChunks.zipWithIndex foreach givenProjectsChunkReturned
      givenProjectsChunkReturned(List.empty[ProjectInfo] -> allDefaultDSProjectsChunks.size)

      defaultOnlyProjects foreach (givenProject(_, existsInNamedGraphs = false))
      bothDSProjects foreach (givenProject(_, existsInNamedGraphs = true))

      defaultOnlyProjects map toRedoEvent foreach givenEventIsSent

      migration.run().value.unsafeRunSync() shouldBe ().asRight
    }

    "return a Recoverable Error if in case of an exception while finding projects " +
      "the given strategy returns one" in new TestCase {

        val exception = exceptions.generateOne

        (defaultGraphProjectsFinder.findDefaultGraphProjects _)
          .expects(1)
          .returning(exception.raiseError[IO, List[ProjectInfo]])

        migration.run().value.unsafeRunSync() shouldBe recoverableError.asLeft
      }
  }

  private trait TestCase {
    val chunkSize = positiveInts(max = 100).generateOne

    val eventCategoryName          = CategoryName("EVENTS_STATUS_CHANGE")
    val defaultGraphProjectsFinder = mock[DefaultGraphProjectsFinder[IO]]
    val namedGraphsProjectFinder   = mock[NamedGraphsProjectFinder[IO]]
    val eventSender                = mock[EventSender[IO]]
    val recoverableError           = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new MigrationToNamedGraphs[IO](defaultGraphProjectsFinder,
                                                   namedGraphsProjectFinder,
                                                   eventSender,
                                                   recoveryStrategy
    )

    def givenProjectsChunkReturned: ((List[ProjectInfo], Int)) => Unit = { case (chunk, idx) =>
      (defaultGraphProjectsFinder.findDefaultGraphProjects _)
        .expects(idx + 1)
        .returning(chunk.pure[IO])
      ()
    }

    def givenProject(project: ProjectInfo, existsInNamedGraphs: Boolean): Unit =
      givenProject(project, existsInNamedGraphs.pure[IO])

    def givenProject(project: ProjectInfo, existsInNamedGraphsResponse: IO[Boolean]): Unit = {
      (namedGraphsProjectFinder.checkExists _)
        .expects(project._1)
        .returning(existsInNamedGraphsResponse)
      ()
    }

    def toRedoEvent(project: ProjectInfo) = project -> EventRequestContent.NoPayload {
      json"""{
        "categoryName": $eventCategoryName,
        "project": {
          "path": ${project._2}
        },
        "newStatus": $TriplesGenerated
      }"""
    }

    lazy val givenEventIsSent: ((ProjectInfo, EventRequestContent.NoPayload)) => Unit = { case ((_, path), event) =>
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

  private lazy val projectInfos: Gen[ProjectInfo] = (projectResourceIds -> projectPaths).mapN(_ -> _)
}

class DefaultGraphProjectsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset {

  private val chunkSize = 50

  "findDefaultGraphProjects" should {

    "find requested page of projects that exists in the Default Graph" in new TestCase {

      val projects = anyRenkuProjectEntities
        .generateList(min = chunkSize + 1, max = Gen.choose(chunkSize + 1, (2 * chunkSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = renkuDataset, projects: _*)

      val (page1, page2) = projects splitAt chunkSize
      finder.findDefaultGraphProjects(page = 1).unsafeRunSync() shouldBe page1.map(p => p.resourceId -> p.path)
      finder.findDefaultGraphProjects(page = 2).unsafeRunSync() shouldBe page2.map(p => p.resourceId -> p.path)
      finder.findDefaultGraphProjects(page = 3).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val finder = new DefaultGraphProjectsFinderImpl[IO](RecordsFinder(renkuDSConnectionInfo))
  }
}

class NamedGraphsProjectFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "checkExists" should {

    "return true if project with the given id exisits in the Named Graphs Dataset" in new TestCase {

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      upload(to = projectsDataset, project)

      (finder checkExists project.resourceId).unsafeRunSync()             shouldBe true
      (finder checkExists projectResourceIds.generateOne).unsafeRunSync() shouldBe false
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val finder = new NamedGraphsProjectFinderImpl[IO](RecordsFinder(projectsDSConnectionInfo))
  }
}
