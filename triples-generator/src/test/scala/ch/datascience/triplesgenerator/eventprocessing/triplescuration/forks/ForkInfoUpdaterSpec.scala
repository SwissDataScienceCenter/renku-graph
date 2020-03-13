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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.Project
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators.{curatedTriplesObjects, curationUpdates}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class ForkInfoUpdaterSpec extends WordSpec with MockFactory {

  "updateForkInfo" should {

    "fetch project from GitLab and from the KG " +
      "and do nothing if both have the same forks" in new TestCase {

      val commonFork    = projectPaths.generateOne
      val gitLabProject = gitLabProjects(commonFork).generateOne
      (gitLabInfoFinder
        .findProject(_: Project)(_: Option[AccessToken]))
        .expects(commit.project, maybeAccessToken)
        .returning(Option(gitLabProject).pure[IO])

      val kgProject = kgProjects(commonFork).generateOne
      (kgInfoFinder
        .findProject(_: Project))
        .expects(commit.project)
        .returning(Option(kgProject).pure[IO])

      updater.updateForkInfo(commit, givenCuratedTriples).unsafeRunSync() shouldBe givenCuratedTriples
    }

    "fetch project from GitLab and from the KG " +
      "and do nothing if both have no forks" in new TestCase {

      val gitLabProject = gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne
      (gitLabInfoFinder
        .findProject(_: Project)(_: Option[AccessToken]))
        .expects(commit.project, maybeAccessToken)
        .returning(Option(gitLabProject).pure[IO])

      val kgProject = kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      (kgInfoFinder
        .findProject(_: Project))
        .expects(commit.project)
        .returning(Option(kgProject).pure[IO])

      updater.updateForkInfo(commit, givenCuratedTriples).unsafeRunSync() shouldBe givenCuratedTriples
    }

    "do nothing if no GitLab project found" in new TestCase {

      (gitLabInfoFinder
        .findProject(_: Project)(_: Option[AccessToken]))
        .expects(commit.project, maybeAccessToken)
        .returning(Option.empty[GitLabProject].pure[IO])

      val kgProject = kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      (kgInfoFinder
        .findProject(_: Project))
        .expects(commit.project)
        .returning(Option(kgProject).pure[IO])

      updater.updateForkInfo(commit, givenCuratedTriples).unsafeRunSync() shouldBe givenCuratedTriples
    }

    "do nothing if no KG project found" in new TestCase {

      val gitLabProject = gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne
      (gitLabInfoFinder
        .findProject(_: Project)(_: Option[AccessToken]))
        .expects(commit.project, maybeAccessToken)
        .returning(Option(gitLabProject).pure[IO])

      (kgInfoFinder
        .findProject(_: Project))
        .expects(commit.project)
        .returning(Option.empty[KGProject].pure[IO])

      updater.updateForkInfo(commit, givenCuratedTriples).unsafeRunSync() shouldBe givenCuratedTriples
    }

    "fetch project from GitLab and from the KG " +
      "and recreate wasDerivedFrom, creator and dateCreated " +
      "if forks from the two sources are different - " +
      "case when creator has an email and it's the same as in the KG" in new TestCase {

      val forkOnGitLab        = projectPaths.generateOne
      val emailInGitLab       = emails.generateSome
      val dateCreatedInGitLab = projectCreatedDates.generateOne
      val gitLabProject = gitLabProjects(forkOnGitLab).generateOne.copy(
        maybeCreator = gitLabCreator(emailInGitLab).generateSome,
        dateCreated  = dateCreatedInGitLab
      )
      (gitLabInfoFinder
        .findProject(_: Project)(_: Option[AccessToken]))
        .expects(commit.project, maybeAccessToken)
        .returning(Option(gitLabProject).pure[IO])

      val kgProject = kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne.copy(
        creator     = kgCreator(emailInGitLab).generateOne,
        dateCreated = dateCreatedInGitLab
      )
      (kgInfoFinder
        .findProject(_: Project))
        .expects(commit.project)
        .returning(Option(kgProject).pure[IO])

      val wasDerivedFromDelete = List(curationUpdates.generateOne)
      (updatesCreator.wasDerivedFromDelete _)
        .expects(kgProject.resourceId)
        .returning(wasDerivedFromDelete)

      val wasDerivedFromInsert = List(curationUpdates.generateOne)
      (updatesCreator.wasDerivedFromInsert _)
        .expects(kgProject.resourceId, forkOnGitLab)
        .returning(wasDerivedFromInsert)

      updater.updateForkInfo(commit, givenCuratedTriples).unsafeRunSync() shouldBe givenCuratedTriples.copy(
        updates = givenCuratedTriples.updates ++ List(
          wasDerivedFromDelete,
          wasDerivedFromInsert
        ).flatten
      )
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val commit              = commits.generateOne
    val givenCuratedTriples = curatedTriplesObjects.generateOne

    val gitLabInfoFinder = mock[GitLabInfoFinder[IO]]
    val kgInfoFinder     = mock[KGInfoFinder[IO]]
    val updatesCreator   = mock[UpdatesCreator]
    val updater          = new IOForkInfoUpdater(gitLabInfoFinder, kgInfoFinder, updatesCreator)
  }
}
